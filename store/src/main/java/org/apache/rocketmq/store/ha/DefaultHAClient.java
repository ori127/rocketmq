/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.DefaultMessageStore;

public class DefaultHAClient extends ServiceThread implements HAClient {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * 最大的可读字节数 4M
     */    
    private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
    /**
     * 未使用?
     */
    private final AtomicReference<String> masterHaAddress = new AtomicReference<>();
    /**
     * 主地址
     */
    private final AtomicReference<String> masterAddress = new AtomicReference<>();
    /**
     * 报告偏移量 8 个字节
     */
    private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
    /**
     * 与Master的SocketChannel
     */
    private SocketChannel socketChannel;
    /**
     * 注册到 Master的SocketChannel 的 selector
     */
    private Selector selector;
    /**
     * 上次从主服务读取数据的时间
     * last time that slave reads date from master.
     */
    private long lastReadTimestamp = System.currentTimeMillis();
    /**
     * 上次备服务报告给主偏移量的时间戳
     * last time that slave reports offset to master.
     */
    private long lastWriteTimestamp = System.currentTimeMillis();
    /**
     * 当前 报告的偏移量
     */
    private long currentReportedOffset = 0;
    /**
     * 记录 byteBufferRead 读取位置
     */
    private int dispatchPosition = 0;
    /**
     * byteBufferRead.position byteBufferRead 写入位置
     */
    private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
    private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
    private DefaultMessageStore defaultMessageStore;
    /**
     * 链接状态
     */
    private volatile HAConnectionState currentState = HAConnectionState.READY;
    /**
     * 流量监控
     */
    private FlowMonitor flowMonitor;

    public DefaultHAClient(DefaultMessageStore defaultMessageStore) throws IOException {
        this.selector = RemotingUtil.openSelector();
        this.defaultMessageStore = defaultMessageStore;
        this.flowMonitor = new FlowMonitor(defaultMessageStore.getMessageStoreConfig());
    }
    /**
     * 更新 HAmaster地址
     */
    public void updateHaMasterAddress(final String newAddr) {
        String currentAddr = this.masterHaAddress.get();
        if (masterHaAddress.compareAndSet(currentAddr, newAddr)) {
            log.info("update master ha address, OLD: " + currentAddr + " NEW: " + newAddr);
        }
    }
    /**
     * 更新 master地址
     */
    public void updateMasterAddress(final String newAddr) {
        String currentAddr = this.masterAddress.get();
        if (masterAddress.compareAndSet(currentAddr, newAddr)) {
            log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
        }
    }

    public String getHaMasterAddress() {
        return this.masterHaAddress.get();
    }

    public String getMasterAddress() {
        return this.masterAddress.get();
    }
    /**
     * 判断是否要报告偏移量的时间
     */
    private boolean isTimeToReportOffset() {
        //计算 上次写时间经过了多久 是否超过发送心跳时间间隔
        long interval = defaultMessageStore.now() - this.lastWriteTimestamp;
        return interval > defaultMessageStore.getMessageStoreConfig().getHaSendHeartbeatInterval();
    }

    private boolean reportSlaveMaxOffset(final long maxOffset) {
        //写入最大的偏移量
        this.reportOffset.position(0);
        this.reportOffset.limit(8);
        this.reportOffset.putLong(maxOffset);
        this.reportOffset.position(0);
        this.reportOffset.limit(8);
        //尝试进行三次将 偏移量进行写入socketChannel
        for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
            try {
                this.socketChannel.write(this.reportOffset);
            } catch (IOException e) {
                log.error(this.getServiceName()
                    + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                return false;
            }
        }
        //记录最近写入时间戳 判断是否写入成功
        lastWriteTimestamp = this.defaultMessageStore.getSystemClock().now();
        return !this.reportOffset.hasRemaining();
    }

    private void reallocateByteBuffer() {
        //剩余可读字节数量    
        int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
        if (remain > 0) {
            //byteBufferRead 定位到 上次调度位置
            this.byteBufferRead.position(this.dispatchPosition);
            //byteBufferRead 剩余的字节 写入 byteBufferBackup
            this.byteBufferBackup.position(0);
            this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
            this.byteBufferBackup.put(this.byteBufferRead);
        }
        //将byteBufferRead 和 byteBufferBackup 进行交换
        this.swapByteBuffer();
        //将byteBufferRead 定位 剩余的空间的位置 重置 dispatchPosition
        this.byteBufferRead.position(remain);
        this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
        this.dispatchPosition = 0;
    }
    /**
     * 交换ByteBuffer 将byteBufferRead 和 byteBufferBackup 进行交换
     */
    private void swapByteBuffer() {
        ByteBuffer tmp = this.byteBufferRead;
        this.byteBufferRead = this.byteBufferBackup;
        this.byteBufferBackup = tmp;
    }

    private boolean processReadEvent() {
        //没有读取字节的次数
        int readSizeZeroTimes = 0;
        //如果byteBufferRead 还有剩余空间 则从 socketChannel 读取
        while (this.byteBufferRead.hasRemaining()) {
            try {
                int readSize = this.socketChannel.read(this.byteBufferRead);
                if (readSize > 0) {
                    //记录读入字节数量
                    flowMonitor.addByteCountTransferred(readSize);
                    readSizeZeroTimes = 0;
                    boolean result = this.dispatchReadRequest();
                    if (!result) {
                        log.error("HAClient, dispatchReadRequest error");
                        return false;
                    }
                    //记录最近读取时间
                    lastReadTimestamp = System.currentTimeMillis();
                } else if (readSize == 0) {
                    //如果超过三次没有读取字节 则进行跳出
                    if (++readSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    log.info("HAClient, processReadEvent read socket < 0");
                    return false;
                }
            } catch (IOException e) {
                log.info("HAClient, processReadEvent read socket exception", e);
                return false;
            }
        }

        return true;
    }

    private boolean dispatchReadRequest() {
        //8个字节的偏移量 4 个字节的 大小
        final int msgHeaderSize = 8 + 4; // phyoffset + size
        int readSocketPos = this.byteBufferRead.position();

        while (true) {
            //当前可读位置 - 调度位置
            int diff = this.byteBufferRead.position() - this.dispatchPosition;
            //超过消息的头信息 8个字节的偏移量 和 4 个字节的大小
            if (diff >= msgHeaderSize) {
                //定位到 上次调度 位置 读取 偏移量 读取大小
                long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);
                //获取从的偏移量 
                long slavePhyOffset = this.defaultMessageStore.getMaxPhyOffset();
                //主的偏移量 和 从 偏移量 不相等    
                if (slavePhyOffset != 0) {
                    if (slavePhyOffset != masterPhyOffset) {
                        log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                            + slavePhyOffset + " MASTER: " + masterPhyOffset);
                        return false;
                    }
                }
                // 有完整的消息 可读    
                if (diff >= (msgHeaderSize + bodySize)) {
                    //将可读的 byteBufferRead 转成 字节数组
                    byte[] bodyData = byteBufferRead.array();
                    int dataStart = this.dispatchPosition + msgHeaderSize;
                    //进行提交
                    this.defaultMessageStore.appendToCommitLog(
                        masterPhyOffset, bodyData, dataStart, bodySize);
                    // FIXME:: 重新定位读取Socket的 位置?
                    this.byteBufferRead.position(readSocketPos);
                    //提交该调度位置 
                    this.dispatchPosition += msgHeaderSize + bodySize;
                    //报告从的最大偏移量
                    if (!reportSlaveMaxOffsetPlus()) {
                        return false;
                    }
                    // 尝试继续读取
                    continue;
                }
            }
            //没有剩余的空间 则重新分配 ByteBuffer
            if (!this.byteBufferRead.hasRemaining()) {
                this.reallocateByteBuffer();
            }

            break;
        }

        return true;
    }

    private boolean reportSlaveMaxOffsetPlus() {
        boolean result = true;
        //获取存储的最大偏移量 
        long currentPhyOffset = this.defaultMessageStore.getMaxPhyOffset();
        if (currentPhyOffset > this.currentReportedOffset) {
            //设置当前偏移量
            this.currentReportedOffset = currentPhyOffset;
            result = this.reportSlaveMaxOffset(this.currentReportedOffset);
            //报告错误偏移量 关关闭与Master的连接
            if (!result) {
                this.closeMaster();
                log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
            }
        }

        return result;
    }

    public void changeCurrentState(HAConnectionState currentState) {
        log.info("change state to {}", currentState);
        this.currentState = currentState;
    }

    public boolean connectMaster() throws ClosedChannelException {
        if (null == socketChannel) {
            String addr = this.masterHaAddress.get();
            if (addr != null) {
                //将masterHaAddress 转成 SocketAddress
                //尝试连接 注册  OP_READ 改变状态
                SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                this.socketChannel = RemotingUtil.connect(socketAddress);
                if (this.socketChannel != null) {
                    this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                    log.info("HAClient connect to master {}", addr);
                    this.changeCurrentState(HAConnectionState.TRANSFER);
                }
            }
            //当前最高的偏移量
            this.currentReportedOffset = this.defaultMessageStore.getMaxPhyOffset();
            //记录从主服务读取数据的时间
            this.lastReadTimestamp = System.currentTimeMillis();
        }

        return this.socketChannel != null;
    }
    /**
     * 关闭与Master的链接
     */
    public void closeMaster() {
        if (null != this.socketChannel) {
            try {
                //进行关闭 关闭channel
                SelectionKey sk = this.socketChannel.keyFor(this.selector);
                if (sk != null) {
                    sk.cancel();
                }

                this.socketChannel.close();

                this.socketChannel = null;
                //将状态改成 READY
                log.info("HAClient close connection with master {}", this.masterHaAddress.get());
                this.changeCurrentState(HAConnectionState.READY);
            } catch (IOException e) {
                log.warn("closeMaster exception. ", e);
            }
            //清空上次读取时间 调度位置 重置byteBufferBackup byteBufferRead 写位置
            this.lastReadTimestamp = 0;
            this.dispatchPosition = 0;

            this.byteBufferBackup.position(0);
            this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

            this.byteBufferRead.position(0);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
        }
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");
        //流量监控
        this.flowMonitor.start();

        while (!this.isStopped()) {
            try {
                switch (this.currentState) {
                    case SHUTDOWN:
                        return;
                    case READY:
                        //尝试进行连接master 连接失败等待 5 s 进行重连
                        if (!this.connectMaster()) {
                            log.warn("HAClient connect to master {} failed", this.masterHaAddress.get());
                            this.waitForRunning(1000 * 5);
                        }
                        continue;
                    case TRANSFER:
                        if (!transferFromMaster()) {
                            closeMasterAndWait();
                            continue;
                        }
                        break;
                    default:
                        this.waitForRunning(1000 * 2);
                        continue;
                }
                //计算从Master 获取时间戳的间隔时间 超过与主的链接时间 关闭与Master链接
                long interval = this.defaultMessageStore.now() - this.lastReadTimestamp;
                if (interval > this.defaultMessageStore.getMessageStoreConfig().getHaHousekeepingInterval()) {
                    log.warn("AutoRecoverHAClient, housekeeping, found this connection[" + this.masterHaAddress
                        + "] expired, " + interval);
                    this.closeMaster();
                    log.warn("AutoRecoverHAClient, master not response some time, so close connection");
                }
            } catch (Exception e) {
                //抛出异常 关闭与主的连接 重新连接
                log.warn(this.getServiceName() + " service has exception. ", e);
                this.closeMasterAndWait();
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    private boolean transferFromMaster() throws IOException {
        boolean result;
        //是否到时时间报告偏移量 偏移量报告
        if (this.isTimeToReportOffset()) {
            log.info("Slave report current offset {}", this.currentReportedOffset);
            result = this.reportSlaveMaxOffset(this.currentReportedOffset);
            if (!result) {
                return false;
            }
        }

        this.selector.select(1000);
        //读取消息
        result = this.processReadEvent();
        if (!result) {
            return false;
        }

        return reportSlaveMaxOffsetPlus();
    }
    /**
     * 关闭与Master的链接 然后等待5秒
     */
    public void closeMasterAndWait() {
        this.closeMaster();
        this.waitForRunning(1000 * 5);
    }

    public long getLastWriteTimestamp() {
        return this.lastWriteTimestamp;
    }

    public long getLastReadTimestamp() {
        return lastReadTimestamp;
    }

    @Override
    public HAConnectionState getCurrentState() {
        return currentState;
    }

    @Override
    public long getTransferredByteInSecond() {
        return flowMonitor.getTransferredByteInSecond();
    }

    @Override
    public void shutdown() {
        //设置关闭状态 关闭流量监控 进行关闭 关闭与Master的链接
        this.changeCurrentState(HAConnectionState.SHUTDOWN);
        this.flowMonitor.shutdown();
        super.shutdown();

        closeMaster();
        try {
            this.selector.close();
        } catch (IOException e) {
            log.warn("Close the selector of AutoRecoverHAClient error, ", e);
        }
    }

    @Override
    public String getServiceName() {
        if (this.defaultMessageStore != null && this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
            return this.defaultMessageStore.getBrokerIdentity().getLoggerIdentifier() + DefaultHAClient.class.getSimpleName();
        }
        return DefaultHAClient.class.getSimpleName();
    }
}
