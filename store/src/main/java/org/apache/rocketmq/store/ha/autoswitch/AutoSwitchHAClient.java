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

package org.apache.rocketmq.store.ha.autoswitch;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.EpochEntry;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.ha.FlowMonitor;
import org.apache.rocketmq.store.ha.HAClient;
import org.apache.rocketmq.store.ha.HAConnectionState;
import org.apache.rocketmq.store.ha.io.AbstractHAReader;
import org.apache.rocketmq.store.ha.io.HAWriter;

public class AutoSwitchHAClient extends ServiceThread implements HAClient {

    /**
     * Handshake header buffer size. Schema: state ordinal + Two flags + slaveAddressLength
     * Flag: isSyncFromLastFile(short), isAsyncLearner(short)... we can add more flags in the future if needed
     * 4 个字节 连接状态枚举值 + 4 个字节 标记 2个字节的 最后一个文件传输  2个字节的异步同步 + 4 字节的本地地址的长度
     */
    public static final int HANDSHAKE_HEADER_SIZE = 4 + 4 + 4;

    /**
     * Header + slaveAddress. 头 + slave 大小
     */
    public static final int HANDSHAKE_SIZE = HANDSHAKE_HEADER_SIZE + 50;

    /**
     * 4 个 字节的 状态 + 8个字节最大偏移量
     * Transfer header buffer size. Schema: state ordinal + maxOffset.
     */
    public static final int TRANSFER_HEADER_SIZE = 4 + 8;
    /**
     * 最小的 头的 大小
     */
    public static final int MIN_HEADER_SIZE = Math.min(HANDSHAKE_HEADER_SIZE, TRANSFER_HEADER_SIZE);
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * 最大为4M
     */
    private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
    /**
     * master地址
     */
    private final AtomicReference<String> masterHaAddress = new AtomicReference<>();
    private final AtomicReference<String> masterAddress = new AtomicReference<>();
    /**
     * salve Id
     */
    private final AtomicReference<Long> slaveId = new AtomicReference<>();
    /**
     * 用作一致性校验 ByteBuffer
     */
    private final ByteBuffer handshakeHeaderBuffer = ByteBuffer.allocate(HANDSHAKE_SIZE);
    /**
     * 用来上报偏移量的 ByteBuffer
     */
    private final ByteBuffer transferHeaderBuffer = ByteBuffer.allocate(TRANSFER_HEADER_SIZE);
    private final AutoSwitchHAService haService;
    /**
     * 接收到 ByteBuffer
     */
    private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
    private final DefaultMessageStore messageStore;
    /**
     * EpochFileCache
     */
    private final EpochFileCache epochCache;
    /**
     * 本地地址
     */
    private String localAddress;
    /**
     * master 的 socketChannel
     */
    private SocketChannel socketChannel;
    /**
     * 注册的 selector
     */
    private Selector selector;
    /**
     * 进行读取 如果是 HANDSHAKE 找到对应的一致的偏移量 然后从该偏移量 继续进行传输
     * 如果是 TRANSFER 则 进行进行传输
     */
    private AbstractHAReader haReader;
    private HAWriter haWriter;
    /**
     * 流量监控
     */
    private FlowMonitor flowMonitor;
    /**
     * last time that slave reads date from master.
     * 上次 slave 从 master 读取时间
     */
    private long lastReadTimestamp;
    /**
     * last time that slave reports offset to master.
     * 上次 slave 像 master 上报的时间
     */
    private long lastWriteTimestamp;
    /**
     * 当前报告的偏移量
     */
    private long currentReportedOffset;
    /**
     * 当前 byteBufferRead 的位置
     */
    private int processPosition;
    /**
     * 连接状态
     */
    private volatile HAConnectionState currentState;
    /**
     * Current epoch 当前代数
     */
    private volatile long currentReceivedEpoch;

    public AutoSwitchHAClient(AutoSwitchHAService haService, DefaultMessageStore defaultMessageStore,
        EpochFileCache epochCache) throws IOException {
        this.haService = haService;
        this.messageStore = defaultMessageStore;
        this.epochCache = epochCache;
        init();
    }

    public void init() throws IOException {
        this.selector = RemotingUtil.openSelector();
        this.flowMonitor = new FlowMonitor(this.messageStore.getMessageStoreConfig());
        this.haReader = new HAClientReader();
        //注册的读取钩子,记录最近 读取时间  进行流量监控
        haReader.registerHook(readSize -> {
            if (readSize > 0) {
                AutoSwitchHAClient.this.flowMonitor.addByteCountTransferred(readSize);
                lastReadTimestamp = System.currentTimeMillis();
            }
        });
        this.haWriter = new HAWriter();
        //注册写钩子
        haWriter.registerHook(writeSize -> {
            if (writeSize > 0) {
                lastWriteTimestamp = System.currentTimeMillis();
            }
        });
        //等带创建连接
        changeCurrentState(HAConnectionState.READY);
        this.currentReceivedEpoch = -1;
        this.currentReportedOffset = 0;
        this.processPosition = 0;
        this.lastReadTimestamp = System.currentTimeMillis();
        this.lastWriteTimestamp = System.currentTimeMillis();
        haService.updateConfirmOffset(-1);
    }

    /**
     * 现关闭然后重新打开
     * @throws IOException
     */
    public void reOpen() throws IOException {
        shutdown();
        init();
    }

    @Override
    public String getServiceName() {
        if (haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) {
            return haService.getDefaultMessageStore().getBrokerIdentity().getLoggerIdentifier() + AutoSwitchHAClient.class.getSimpleName();
        }
        return AutoSwitchHAClient.class.getSimpleName();
    }

    public void setLocalAddress(String localAddress) {
        this.localAddress = localAddress;
    }

    public void updateSlaveId(Long newId) {
        Long currentId = this.slaveId.get();
        if (this.slaveId.compareAndSet(currentId, newId)) {
            LOGGER.info("Update slave Id, OLD: {}, New: {}", currentId, newId);
        }
    }

    @Override
    public void updateMasterAddress(String newAddress) {
        String currentAddr = this.masterAddress.get();
        if (!StringUtils.equals(newAddress, currentAddr) && masterAddress.compareAndSet(currentAddr, newAddress)) {
            LOGGER.info("update master address, OLD: " + currentAddr + " NEW: " + newAddress);
        }
    }

    @Override
    public void updateHaMasterAddress(String newAddress) {
        String currentAddr = this.masterHaAddress.get();
        if (!StringUtils.equals(newAddress, currentAddr) && masterHaAddress.compareAndSet(currentAddr, newAddress)) {
            LOGGER.info("update master ha address, OLD: " + currentAddr + " NEW: " + newAddress);
            wakeup();
        }
    }

    @Override
    public String getMasterAddress() {
        return this.masterAddress.get();
    }

    @Override
    public String getHaMasterAddress() {
        return this.masterHaAddress.get();
    }

    @Override
    public long getLastReadTimestamp() {
        return this.lastReadTimestamp;
    }

    @Override
    public long getLastWriteTimestamp() {
        return this.lastWriteTimestamp;
    }

    @Override
    public HAConnectionState getCurrentState() {
        return this.currentState;
    }

    @Override
    public void changeCurrentState(HAConnectionState haConnectionState) {
        LOGGER.info("change state to {}", haConnectionState);
        this.currentState = haConnectionState;
    }

    public void closeMasterAndWait() {
        this.closeMaster();
        this.waitForRunning(1000 * 5);
    }

    @Override
    public void closeMaster() {
        if (null != this.socketChannel) {
            try {
                //取消 selector 注册的key
                SelectionKey sk = this.socketChannel.keyFor(this.selector);
                if (sk != null) {
                    sk.cancel();
                }
                //关闭与 master 的 socketChannel
                this.socketChannel.close();
                this.socketChannel = null;
                //将状态设置成 READY
                LOGGER.info("AutoSwitchHAClient close connection with master {}", this.masterHaAddress.get());
                this.changeCurrentState(HAConnectionState.READY);
            } catch (IOException e) {
                LOGGER.warn("CloseMaster exception. ", e);
            }

            this.lastReadTimestamp = 0;
            this.processPosition = 0;

            this.byteBufferRead.position(0);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
        }
    }

    /**
     * 获取一秒内传输的字节数量
     * @return
     */
    @Override
    public long getTransferredByteInSecond() {
        return this.flowMonitor.getTransferredByteInSecond();
    }

    @Override
    public void shutdown() {
        //设置状态为关闭
        changeCurrentState(HAConnectionState.SHUTDOWN);
        // Shutdown thread firstly
        //关闭流量监控 关闭服务线程
        this.flowMonitor.shutdown();
        super.shutdown();
        //与Master 关闭连接
        closeMaster();
        try {
            this.selector.close();
        } catch (IOException e) {
            LOGGER.warn("Close the selector of AutoSwitchHAClient error, ", e);
        }
    }

    /**
     * 是否是时间需要报告偏移量
     * @return
     */
    private boolean isTimeToReportOffset() {
        //计算上次的写入时间是否超过时间间隔
        long interval = this.messageStore.now() - this.lastWriteTimestamp;
        return interval > this.messageStore.getMessageStoreConfig().getHaSendHeartbeatInterval();
    }

    /**
     * 发送头部信息
     * @return
     * @throws IOException
     */
    private boolean sendHandshakeHeader() throws IOException {
        this.handshakeHeaderBuffer.position(0);
        this.handshakeHeaderBuffer.limit(HANDSHAKE_SIZE);
        // Original state 4个字节 salve 字节的状态
        this.handshakeHeaderBuffer.putInt(HAConnectionState.HANDSHAKE.ordinal());
        // IsSyncFromLastFile 2个字节 是否 最后一个文件传输
        short isSyncFromLastFile = this.haService.getDefaultMessageStore().getMessageStoreConfig().isSyncFromLastFile() ? (short) 1 : (short) 0;
        this.handshakeHeaderBuffer.putShort(isSyncFromLastFile);
        // IsAsyncLearner role  2个字节 是否异步同步
        short isAsyncLearner = this.haService.getDefaultMessageStore().getMessageStoreConfig().isAsyncLearner() ? (short) 1 : (short) 0;
        this.handshakeHeaderBuffer.putShort(isAsyncLearner);
        // slave 地址大小
        // Address length 4个字节 本地字节字节长度
        this.handshakeHeaderBuffer.putInt(this.localAddress == null ? 0 : this.localAddress.length());
        // slave 地址 本地地址
        // Slave address
        this.handshakeHeaderBuffer.put(this.localAddress == null ? new byte[0] : this.localAddress.getBytes(StandardCharsets.UTF_8));
        this.handshakeHeaderBuffer.flip();
        //写出handshakeHeaderBuffer
        return this.haWriter.write(this.socketChannel, this.handshakeHeaderBuffer);
    }

    private void handshakeWithMaster() throws IOException {
        boolean result = this.sendHandshakeHeader();
        //失败关闭连接
        if (!result) {
            closeMasterAndWait();
        }

        this.selector.select(5000);
        //读取失败关闭连接
        result = this.haReader.read(this.socketChannel, this.byteBufferRead);
        if (!result) {
            closeMasterAndWait();
        }
    }

    private boolean reportSlaveOffset(final long offsetToReport) throws IOException {
        //上报 Slave 的偏移量 4个字节的salve 状态 + 8 个字节的 偏移量
        this.transferHeaderBuffer.position(0);
        this.transferHeaderBuffer.limit(TRANSFER_HEADER_SIZE);
        this.transferHeaderBuffer.putInt(this.currentState.ordinal());
        this.transferHeaderBuffer.putLong(offsetToReport);
        this.transferHeaderBuffer.flip();
        return this.haWriter.write(this.socketChannel, this.transferHeaderBuffer);
    }

    private boolean reportSlaveMaxOffset() throws IOException {
        boolean result = true;
        final long maxPhyOffset = this.messageStore.getMaxPhyOffset();
        //最大的物理偏移量 超过当前报告的偏移量 上报偏移量
        if (maxPhyOffset > this.currentReportedOffset) {
            this.currentReportedOffset = maxPhyOffset;
            result = reportSlaveOffset(this.currentReportedOffset);
        }
        return result;
    }

    public boolean connectMaster() throws IOException {
        //与master socketChannel 重新进了连接
        if (null == this.socketChannel) {
            //与master 建立连接 注册读事件 将状态改成 HANDSHAKE
            String addr = this.masterHaAddress.get();
            if (StringUtils.isNotEmpty(addr)) {
                SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                this.socketChannel = RemotingUtil.connect(socketAddress);
                if (this.socketChannel != null) {
                    this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                    LOGGER.info("AutoSwitchHAClient connect to master {}", addr);
                    changeCurrentState(HAConnectionState.HANDSHAKE);
                }
            }
            //更新 上报的偏移 更新最近读取时间
            this.currentReportedOffset = this.messageStore.getMaxPhyOffset();
            this.lastReadTimestamp = System.currentTimeMillis();
        }
        return this.socketChannel != null;
    }

    private boolean transferFromMaster() throws IOException {
        boolean result;
        //是否需要报告偏移量 如果需要则进行报告
        if (isTimeToReportOffset()) {
            LOGGER.info("Slave report current offset {}", this.currentReportedOffset);
            result = reportSlaveOffset(this.currentReportedOffset);
            if (!result) {
                return false;
            }
        }

        this.selector.select(1000);
        //进行读取
        result = this.haReader.read(this.socketChannel, this.byteBufferRead);
        if (!result) {
            return false;
        }

        return this.reportSlaveMaxOffset();
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service started");

        this.flowMonitor.start();
        while (!this.isStopped()) {
            try {
                switch (this.currentState) {
                    case SHUTDOWN:
                        return;
                    case READY:
                        // Truncate invalid msg first
                        final long truncateOffset = AutoSwitchHAClient.this.haService.truncateInvalidMsg();
                        if (truncateOffset >= 0) {
                            AutoSwitchHAClient.this.epochCache.truncateSuffixByOffset(truncateOffset);
                        }
                        // 与master 建立 链接
                        if (!connectMaster()) {
                            LOGGER.warn("AutoSwitchHAClient connect to master {} failed", this.masterHaAddress.get());
                            waitForRunning(1000 * 5);
                        }
                        continue;
                    case HANDSHAKE:
                        // 与 master 进行 一致性检查
                        handshakeWithMaster();
                        continue;
                    case TRANSFER:
                        //传输报告偏移量 传输 CommitLog
                        if (!transferFromMaster()) {
                            closeMasterAndWait();
                            continue;
                        }
                        break;
                    case SUSPEND:
                    default:
                        waitForRunning(1000 * 5);
                        continue;
                }
                long interval = this.messageStore.now() - this.lastReadTimestamp;
                if (interval > this.messageStore.getMessageStoreConfig().getHaHousekeepingInterval()) {
                    LOGGER.warn("AutoSwitchHAClient, housekeeping, found this connection[" + this.masterHaAddress
                        + "] expired, " + interval);
                    closeMaster();
                    LOGGER.warn("AutoSwitchHAClient, master not response some time, so close connection");
                }
            } catch (Exception e) {
                LOGGER.warn(this.getServiceName() + " service has exception. ", e);
                closeMasterAndWait();
            }
        }

    }

    /**
     * 比较主从的 epoch 文件，找到一致点，丢弃。
     * Compare the master and slave's epoch file, find consistent point, do truncate.
     */
    private boolean doTruncate(List<EpochEntry> masterEpochEntries, long masterEndOffset) throws IOException {
        //如果 epochCache 为空 则表示当前 节点 是新 复制 节点 则直接进行传输就可以
        if (this.epochCache.getEntrySize() == 0) {
            // If epochMap is empty, means the broker is a new replicas
            LOGGER.info("Slave local epochCache is empty, skip truncate log");
            changeCurrentState(HAConnectionState.TRANSFER);
            this.currentReportedOffset = 0;
        } else {
            //master 的 EpochCache
            final EpochFileCache masterEpochCache = new EpochFileCache();
            masterEpochCache.initCacheFromEntries(masterEpochEntries);
            masterEpochCache.setLastEpochEntryEndOffset(masterEndOffset);
            final List<EpochEntry> localEpochEntries = this.epochCache.getAllEntries();
            //slave 的  EpochCache
            final EpochFileCache localEpochCache = new EpochFileCache();
            localEpochCache.initCacheFromEntries(localEpochEntries);
            localEpochCache.setLastEpochEntryEndOffset(this.messageStore.getMaxPhyOffset());
            //找到对应的被截断的偏移量 如果截断便宜了 为负数 找不到一致点
            final long truncateOffset = localEpochCache.findConsistentPoint(masterEpochCache);
            if (truncateOffset < 0) {
                // If truncateOffset < 0, means we can't find a consistent point
                LOGGER.error("Failed to find a consistent point between masterEpoch:{} and slaveEpoch:{}", masterEpochEntries, localEpochEntries);
                return false;
            }
            //截断 truncateOffset 之后
            if (!this.messageStore.truncateFiles(truncateOffset)) {
                LOGGER.error("Failed to truncate slave log to {}", truncateOffset);
                return false;
            }
            //截断 truncateOffset 之后的 EpochEntry
            this.epochCache.truncateSuffixByOffset(truncateOffset);
            LOGGER.info("Truncate slave log to {} success, change to transfer state", truncateOffset);
            //修改连接状态
            changeCurrentState(HAConnectionState.TRANSFER);
            //从截断处开始 继续传输
            this.currentReportedOffset = truncateOffset;
        }
        if (!reportSlaveMaxOffset()) {
            LOGGER.error("AutoSwitchHAClient report max offset to master failed");
            return false;
        }
        return true;
    }

    class HAClientReader extends AbstractHAReader {
        /**
         * 进行读取 如果是 HANDSHAKE 找到对应的一致的偏移量 然后从该偏移量 继续进行传输
         * 如果是 TRANSFER 则 进行进行传输
         * @param byteBufferRead read result
         * @return
         */
        @Override
        protected boolean processReadResult(ByteBuffer byteBufferRead) {
            int readSocketPos = byteBufferRead.position();
            try {
                while (true) {

                    int diff = byteBufferRead.position() - AutoSwitchHAClient.this.processPosition;
                    //如果数据超过消息头的大小
                    if (diff >= AutoSwitchHAConnection.MSG_HEADER_SIZE) {
                        int processPosition = AutoSwitchHAClient.this.processPosition;
                        //4个 字节的 master 状态
                        int masterState = byteBufferRead.getInt(processPosition + AutoSwitchHAConnection.MSG_HEADER_SIZE - 36);
                        //4个字节 所有epochEntries 大小
                        int bodySize = byteBufferRead.getInt(processPosition + AutoSwitchHAConnection.MSG_HEADER_SIZE - 32);
                        //8个字节 master的偏移量
                        long masterOffset = byteBufferRead.getLong(processPosition + AutoSwitchHAConnection.MSG_HEADER_SIZE - 28);
                        //4个字节 master的Epoch
                        int masterEpoch = byteBufferRead.getInt(processPosition + AutoSwitchHAConnection.MSG_HEADER_SIZE - 20);
                        //8个字节 master 的 Epoch 开始偏移量
                        long masterEpochStartOffset = byteBufferRead.getLong(processPosition + AutoSwitchHAConnection.MSG_HEADER_SIZE - 16);
                        // 8个字节 master 提交的偏移量
                        long confirmOffset = byteBufferRead.getLong(processPosition + AutoSwitchHAConnection.MSG_HEADER_SIZE - 8);
                        //如果状态不一致
                        if (masterState != AutoSwitchHAClient.this.currentState.ordinal()) {
                            AutoSwitchHAClient.this.processPosition += AutoSwitchHAConnection.MSG_HEADER_SIZE + bodySize;
                            AutoSwitchHAClient.this.waitForRunning(1);
                            LOGGER.error("State not matched, masterState:{}, slaveState:{}, bodySize:{}, offset:{}, masterEpoch:{}, masterEpochStartOffset:{}, confirmOffset:{}",
                                masterState, AutoSwitchHAClient.this.currentState, bodySize, masterOffset, masterEpoch, masterEpochStartOffset, confirmOffset);
                            return true;
                        }
                        //剩余的数据超过 body 大小
                        if (diff >= (AutoSwitchHAConnection.MSG_HEADER_SIZE + bodySize)) {
                            switch (AutoSwitchHAClient.this.currentState) {
                                case HANDSHAKE:
                                    AutoSwitchHAClient.this.processPosition += AutoSwitchHAConnection.MSG_HEADER_SIZE;
                                    // Truncate log EPOCH_ENTRY大小
                                    int entrySize = AutoSwitchHAConnection.EPOCH_ENTRY_SIZE;
                                    //计算 EPOCH_ENTRY  的数量
                                    final int entryNums = bodySize / entrySize;
                                    final ArrayList<EpochEntry> epochEntries = new ArrayList<>(entryNums);
                                    for (int i = 0; i < entryNums; i++) {
                                        // epoch 和 开始偏移量
                                        int epoch = byteBufferRead.getInt(AutoSwitchHAClient.this.processPosition + i * entrySize);
                                        long startOffset = byteBufferRead.getLong(AutoSwitchHAClient.this.processPosition + i * entrySize + 4);
                                        epochEntries.add(new EpochEntry(epoch, startOffset));
                                    }
                                    //将byteBufferRead 定位到最后的位置 processPosition 定位到处理为止
                                    byteBufferRead.position(readSocketPos);
                                    AutoSwitchHAClient.this.processPosition += bodySize;
                                    LOGGER.info("Receive handshake, masterMaxPosition {}, masterEpochEntries:{}, try truncate log", masterOffset, epochEntries);
                                    //比较主从的 epoch 文件，找到一致点，丢弃。
                                    if (!doTruncate(epochEntries, masterOffset)) {
                                        waitForRunning(1000 * 2);
                                        LOGGER.error("AutoSwitchHAClient truncate log failed in handshake state");
                                        return false;
                                    }
                                    break;
                                case TRANSFER:
                                    //分配 body字节 定位到数开始位置 进行读取
                                    byte[] bodyData = new byte[bodySize];
                                    byteBufferRead.position(AutoSwitchHAClient.this.processPosition + AutoSwitchHAConnection.MSG_HEADER_SIZE);
                                    byteBufferRead.get(bodyData);
                                    //将byteBufferRead 定位到最后的位置 processPosition 定位到处理为止
                                    byteBufferRead.position(readSocketPos);
                                    AutoSwitchHAClient.this.processPosition += AutoSwitchHAConnection.MSG_HEADER_SIZE + bodySize;
                                    //获取 slave 的最大偏移量 如果 和 master 不一致
                                    long slavePhyOffset = AutoSwitchHAClient.this.messageStore.getMaxPhyOffset();
                                    if (slavePhyOffset != 0) {
                                        if (slavePhyOffset != masterOffset) {
                                            LOGGER.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                                + slavePhyOffset + " MASTER: " + masterOffset);
                                            return false;
                                        }
                                    }

                                    // If epoch changed
                                    // 如果 master的 Epoch 和当前 收到 Epoch 不一致 则 新加新的 Epoch
                                    if (masterEpoch != AutoSwitchHAClient.this.currentReceivedEpoch) {
                                        AutoSwitchHAClient.this.currentReceivedEpoch = masterEpoch;
                                        AutoSwitchHAClient.this.epochCache.appendEntry(new EpochEntry(masterEpoch, masterEpochStartOffset));
                                    }
                                    //如果body 大于 0 则进行存储
                                    if (bodySize > 0) {
                                        AutoSwitchHAClient.this.messageStore.appendToCommitLog(masterOffset, bodyData, 0, bodyData.length);
                                    }
                                    //更新提交的偏移量
                                    haService.updateConfirmOffset(Math.min(confirmOffset, messageStore.getMaxPhyOffset()));
                                    //重新上报偏移量
                                    if (!reportSlaveMaxOffset()) {
                                        LOGGER.error("AutoSwitchHAClient report max offset to master failed");
                                        return false;
                                    }
                                    break;
                                default:
                                    break;
                            }
                            continue;
                        }
                    }
                    //如果没有剩余可读内容 定位处理的位置 将剩余的数据 重置到 byteBufferRead 开始位置
                    if (!byteBufferRead.hasRemaining()) {
                        byteBufferRead.position(AutoSwitchHAClient.this.processPosition);
                        byteBufferRead.compact();
                        AutoSwitchHAClient.this.processPosition = 0;
                    }

                    break;
                }
            } catch (final Exception e) {
                LOGGER.error("Error when ha client process read request", e);
            }
            return true;
        }
    }
}
