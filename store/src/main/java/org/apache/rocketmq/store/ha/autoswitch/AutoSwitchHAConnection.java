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
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.rocketmq.common.EpochEntry;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.FlowMonitor;
import org.apache.rocketmq.store.ha.HAConnection;
import org.apache.rocketmq.store.ha.HAConnectionState;
import org.apache.rocketmq.store.ha.io.AbstractHAReader;
import org.apache.rocketmq.store.ha.io.HAWriter;

public class AutoSwitchHAConnection implements HAConnection {
    /**
     * Header protocol in syncing msg from master. Format: current state + body size + offset + epoch  +
     * epochStartOffset + additionalInfo(confirmOffset). If the msg is handShakeMsg, the body size = EpochEntrySize *
     * EpochEntryNums, the offset is maxOffset in master.
     * 4 字节连接状态枚举值 + 4 字节 所有epochEntries 大小  + 8 字节的最大 偏移量 + 4个字节 最大 Epoch + 8个 字节 Epoch 开始偏移量 + 8 个字节 附加信息 确认的偏移量
     */
    public static final int MSG_HEADER_SIZE = 4 + 4 + 8 + 4 + 8 + 8;
    /**
     * EpochEntry的大小 4个字节 代数 8个 字节的开始 偏移量 结束偏移量由下 entry 进行计算
     */
    public static final int EPOCH_ENTRY_SIZE = 12;
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final AutoSwitchHAService haService;
    /**
     * 客户端对应的SocketChannel
     */
    private final SocketChannel socketChannel;
    /**
     * 客户端地址
     */
    private final String clientAddress;
    /**
     * EpochFile
     */
    private final EpochFileCache epochCache;
    /**
     * 写服务
     */
    private final AbstractWriteSocketService writeSocketService;
    /**
     * 读服务
     */
    private final ReadSocketService readSocketService;
    /**
     * 流量监控
     */
    private final FlowMonitor flowMonitor;
    /**
     * 连接状态
     */
    private volatile HAConnectionState currentState = HAConnectionState.HANDSHAKE;
    /**
     * salve 请求的偏移量
     */
    private volatile long slaveRequestOffset = -1;
    /**
     * slave 的 确认的偏移量
     */
    private volatile long slaveAckOffset = -1;
    /**
     * Whether the slave have already sent a handshake message
     *  从 是否已经设置 发送 handshake 消息
     */
    private volatile boolean isSlaveSendHandshake = false;
    /**
     * 当前传输的Epoch
     */
    private volatile int currentTransferEpoch = -1;
    /**
     * 当前传输的 Epoch 结束偏移 -1 表示为最后一个 Epoch
     */
    private volatile long currentTransferEpochEndOffset = 0;
    /**
     * 是否从最后一个文件开始 同步
     */
    private volatile boolean isSyncFromLastFile = false;
    /**
     * 是否异步进行同步
     */
    private volatile boolean isAsyncLearner = false;
    /***
     * slave ID
     */
    private volatile long slaveId = -1;
    /**
     * salve 地址
     */
    private volatile String slaveAddress;

    /**
     * 最近的 结束偏移量 master 传输到 slave
     * Last endOffset when master transfer data to slave
     */
    private volatile long lastMasterMaxOffset = -1;
    /**
     * 最近传输到 salve 的 传输时间
     * Last time ms when transfer data to slave.
     */
    private volatile long lastTransferTimeMs = 0;

    public AutoSwitchHAConnection(AutoSwitchHAService haService, SocketChannel socketChannel,
        EpochFileCache epochCache) throws IOException {
        this.haService = haService;
        this.socketChannel = socketChannel;
        this.epochCache = epochCache;
        this.clientAddress = this.socketChannel.socket().getRemoteSocketAddress().toString();
        this.socketChannel.configureBlocking(false);
        this.socketChannel.socket().setSoLinger(false, -1);
        this.socketChannel.socket().setTcpNoDelay(true);
        if (NettySystemConfig.socketSndbufSize > 0) {
            //设置接收Buffer
            this.socketChannel.socket().setReceiveBufferSize(NettySystemConfig.socketSndbufSize);
        }
        if (NettySystemConfig.socketRcvbufSize > 0) {
            //设置发送buffer
            this.socketChannel.socket().setSendBufferSize(NettySystemConfig.socketRcvbufSize);
        }
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        this.readSocketService = new ReadSocketService(this.socketChannel);
        this.haService.getConnectionCount().incrementAndGet();
        this.flowMonitor = new FlowMonitor(haService.getDefaultMessageStore().getMessageStoreConfig());
    }

    @Override
    public void start() {
        changeCurrentState(HAConnectionState.HANDSHAKE);
        this.flowMonitor.start();
        this.readSocketService.start();
        this.writeSocketService.start();
    }

    @Override
    public void shutdown() {
        changeCurrentState(HAConnectionState.SHUTDOWN);
        this.flowMonitor.shutdown(true);
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }

    @Override
    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (final IOException e) {
                LOGGER.error("", e);
            }
        }
    }

    public void changeCurrentState(HAConnectionState connectionState) {
        LOGGER.info("change state to {}", connectionState);
        this.currentState = connectionState;
    }

    public long getSlaveId() {
        return slaveId;
    }

    public String getSlaveAddress() {
        return slaveAddress;
    }

    @Override
    public HAConnectionState getCurrentState() {
        return currentState;
    }

    @Override
    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    @Override
    public String getClientAddress() {
        return clientAddress;
    }

    @Override
    public long getSlaveAckOffset() {
        return slaveAckOffset;
    }

    @Override
    public long getTransferredByteInSecond() {
        return flowMonitor.getTransferredByteInSecond();
    }

    @Override
    public long getTransferFromWhere() {
        return this.writeSocketService.getNextTransferFromWhere();
    }

    /**
     * 将
     * @param entry
     */
    private void changeTransferEpochToNext(final EpochEntry entry) {
        this.currentTransferEpoch = entry.getEpoch();
        this.currentTransferEpochEndOffset = entry.getEndOffset();
        if (entry.getEpoch() == this.epochCache.lastEpoch()) {
            // Use -1 to stand for Long.max
            this.currentTransferEpochEndOffset = -1;
        }
    }

    public boolean isAsyncLearner() {
        return isAsyncLearner;
    }

    public boolean isSyncFromLastFile() {
        return isSyncFromLastFile;
    }

    private synchronized void updateLastTransferInfo() {
        this.lastMasterMaxOffset = this.haService.getDefaultMessageStore().getMaxPhyOffset();
        this.lastTransferTimeMs = System.currentTimeMillis();
    }

    private synchronized void maybeExpandInSyncStateSet(long slaveMaxOffset) {
        //不是 异步同步 并且 slave 的 最大 偏移量 超过 master 最大偏移量
        if (!this.isAsyncLearner && slaveMaxOffset >= this.lastMasterMaxOffset) {
            //计算 追赶 时间 记录 最近追赶上的时间
            long caughtUpTimeMs = this.haService.getDefaultMessageStore().getMaxPhyOffset() == slaveMaxOffset ? System.currentTimeMillis() : this.lastTransferTimeMs;
            this.haService.updateConnectionLastCaughtUpTime(this.slaveAddress, caughtUpTimeMs);
            this.haService.maybeExpandInSyncStateSet(this.slaveAddress, slaveMaxOffset);
        }
    }

    class ReadSocketService extends ServiceThread {
        /**
         * 1MB大小
         */
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
        /**
         * 绑定的 selector
         */
        private final Selector selector;
        /**
         * 客户端的 SocketChannel
         */
        private final SocketChannel socketChannel;
        /**
         * 1MB大小
         */
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        private final AbstractHAReader haReader;
        /**
         * 处理位置
         */
        private int processPosition = 0;
        /**
         * 最近读取时间
         */
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            //socketChannel注册读事件
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.setDaemon(true);
            haReader = new HAServerReader();
            //注册钩子 记录最近读取记录
            haReader.registerHook(readSize -> {
                if (readSize > 0) {
                    ReadSocketService.this.lastReadTimestamp =
                        haService.getDefaultMessageStore().getSystemClock().now();
                }
            });
        }

        @Override
        public void run() {
            LOGGER.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);
                    //从 socketChannel 进行读取
                    boolean ok = this.haReader.read(this.socketChannel, this.byteBufferRead);
                    if (!ok) {
                        AutoSwitchHAConnection.LOGGER.error("processReadEvent error");
                        break;
                    }
                    //超过读取间隔 时间
                    long interval = haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    if (interval > haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        LOGGER.warn("ha housekeeping, found this connection[" + clientAddress + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    AutoSwitchHAConnection.LOGGER.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            this.makeStop();
            //将状态设置关闭
            changeCurrentState(HAConnectionState.SHUTDOWN);
            //关闭写服务
            writeSocketService.makeStop();
            //从haService 移除该链接
            haService.removeConnection(AutoSwitchHAConnection.this);
            //减少是链接计数
            haService.getConnectionCount().decrementAndGet();
            //取消 SelectionKey 的注册
            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                //关闭 selector 和  socketChannel
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                AutoSwitchHAConnection.LOGGER.error("", e);
            }

            AutoSwitchHAConnection.LOGGER.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            if (haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) {
                return haService.getDefaultMessageStore().getBrokerIdentity().getLoggerIdentifier() + ReadSocketService.class.getSimpleName();
            }
            return ReadSocketService.class.getSimpleName();
        }

        class HAServerReader extends AbstractHAReader {
            @Override
            protected boolean processReadResult(ByteBuffer byteBufferRead) {
                while (true) {
                    boolean processSuccess = true;
                    int readSocketPos = byteBufferRead.position();
                    int diff = byteBufferRead.position() - ReadSocketService.this.processPosition;
                    if (diff >= AutoSwitchHAClient.MIN_HEADER_SIZE) {
                        int readPosition = ReadSocketService.this.processPosition;
                        //获取 slave 的 状态
                        HAConnectionState slaveState = HAConnectionState.values()[byteBufferRead.getInt(readPosition)];
                        //slave 的 状态
                        switch (slaveState) {
                            case HANDSHAKE:
                                // AddressLength
                                // 4个字节 地址长度
                                int addressLength = byteBufferRead.getInt(readPosition + AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE - 4);
                                if (diff < AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE + addressLength) {
                                    processSuccess = false;
                                    break;
                                }
                                // Flag(isSyncFromLastFile)
                                //2个字节 是否 从最后一个文件 开始同步
                                short syncFromLastFileFlag = byteBufferRead.getShort(readPosition + AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE - 8);
                                if (syncFromLastFileFlag == 1) {
                                    AutoSwitchHAConnection.this.isSyncFromLastFile = true;
                                }
                                // Flag(isAsyncLearner role)
                                // 2 字节是否 是异步同步
                                short isAsyncLearner = byteBufferRead.getShort(readPosition + AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE - 6);
                                if (isAsyncLearner == 1) {
                                    AutoSwitchHAConnection.this.isAsyncLearner = true;
                                }
                                // Address salve 地址
                                final byte[] addressData = new byte[addressLength];
                                byteBufferRead.position(readPosition + AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE);
                                byteBufferRead.get(addressData);
                                AutoSwitchHAConnection.this.slaveAddress = new String(addressData, StandardCharsets.UTF_8);
                                //标记 slave 已经 发送 握手信息 过来了
                                isSlaveSendHandshake = true;
                                //重新定位 到 读取的位置
                                byteBufferRead.position(readSocketPos);
                                //记录处理 位置
                                ReadSocketService.this.processPosition += AutoSwitchHAClient.HANDSHAKE_HEADER_SIZE + addressLength;
                                LOGGER.info("Receive slave handshake, slaveAddress:{}, isSyncFromLastFile:{}, isAsyncLearner:{}",
                                    AutoSwitchHAConnection.this.slaveAddress, AutoSwitchHAConnection.this.isSyncFromLastFile, AutoSwitchHAConnection.this.isAsyncLearner);
                                break;
                            case TRANSFER:
                                // slave 最大偏移量  这边 + 4  前 4 个字节 表示 slave 状态
                                long slaveMaxOffset = byteBufferRead.getLong(readPosition + 4);
                                // 记录处理 的 位置
                                ReadSocketService.this.processPosition += AutoSwitchHAClient.TRANSFER_HEADER_SIZE;

                                AutoSwitchHAConnection.this.slaveAckOffset = slaveMaxOffset;
                                if (slaveRequestOffset < 0) {
                                    slaveRequestOffset = slaveMaxOffset;
                                }
                                //重新定位 到 读取的位置
                                byteBufferRead.position(readSocketPos);
                                maybeExpandInSyncStateSet(slaveMaxOffset);
                                AutoSwitchHAConnection.this.haService.updateConfirmOffsetWhenSlaveAck(AutoSwitchHAConnection.this.slaveAddress);
                                AutoSwitchHAConnection.this.haService.notifyTransferSome(AutoSwitchHAConnection.this.slaveAckOffset);
                                break;
                            default:
                                LOGGER.error("Current state illegal {}", currentState);
                                break;
                        }

                        if (!slaveState.equals(currentState)) {
                            LOGGER.warn("Master change state from {} to {}", currentState, slaveState);
                            changeCurrentState(slaveState);
                        }
                        if (processSuccess) {
                            continue;
                        }
                    }

                    if (!byteBufferRead.hasRemaining()) {
                        byteBufferRead.position(ReadSocketService.this.processPosition);
                        byteBufferRead.compact();
                        ReadSocketService.this.processPosition = 0;
                    }
                    break;
                }

                return true;
            }
        }
    }

    class WriteSocketService extends AbstractWriteSocketService {
        private SelectMappedBufferResult selectMappedBufferResult;

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            super(socketChannel);
        }

        /**
         * 根据该偏移量来获取对应大小,设置对应的 selectMappedBufferResult
         * @return
         */
        @Override
        protected int getNextTransferDataSize() {
            SelectMappedBufferResult selectResult = haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
            if (selectResult == null || selectResult.getSize() <= 0) {
                return 0;
            }
            this.selectMappedBufferResult = selectResult;
            return selectResult.getSize();
        }

        /**
         * 释放对应的 selectMappedBufferResult
         */
        @Override
        protected void releaseData() {
            this.selectMappedBufferResult.release();
            this.selectMappedBufferResult = null;
        }

        @Override
        protected boolean transferData(int maxTransferSize) throws Exception {
            //限制最大传输大小的偏移量
            if (null != this.selectMappedBufferResult && maxTransferSize >= 0) {
                this.selectMappedBufferResult.getByteBuffer().limit(maxTransferSize);
            }
            // 写头信息
            // Write Header
            boolean result = haWriter.write(this.socketChannel, this.byteBufferHeader);

            if (!result) {
                return false;
            }

            if (null == this.selectMappedBufferResult) {
                return true;
            }
            //写 body 信息
            // Write Body
            result = haWriter.write(this.socketChannel, this.selectMappedBufferResult.getByteBuffer());
            //写入成功释放资源
            if (result) {
                releaseData();
            }
            return result;
        }

        @Override
        protected void onStop() {
            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }
        }

        @Override
        public String getServiceName() {
            if (haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) {
                return haService.getDefaultMessageStore().getBrokerIdentity().getLoggerIdentifier() + WriteSocketService.class.getSimpleName();
            }
            return WriteSocketService.class.getSimpleName();
        }
    }

    abstract class AbstractWriteSocketService extends ServiceThread {
        /**
         * 绑定 selector
         */
        protected final Selector selector;
        /***
         * 客户端的socketChannel
         */
        protected final SocketChannel socketChannel;
        protected final HAWriter haWriter;
        /**
         * 头信息
         * 4 字节连接状态枚举值 + 4 字节 所有epochEntries 大小  + 8 字节的最大 偏移量 + 4个字节 最大 Epoch + 8个 字节 Epoch 开始偏移量 + 8 个字节 附加信息 确认的偏移量
         */
        protected final ByteBuffer byteBufferHeader = ByteBuffer.allocate(MSG_HEADER_SIZE);
        // Store master epochFileCache: (Epoch + startOffset) * 1000
        /**
         * 缓存 master 的 epochFileCache为1000个
         */
        private final ByteBuffer handShakeBuffer = ByteBuffer.allocate(EPOCH_ENTRY_SIZE * 1000);
        /**
         * 下次要从何处开始传输字节
         */
        protected long nextTransferFromWhere = -1;
        /**
         * 上次是否完成写出
         */
        protected boolean lastWriteOver = true;
        /**
         * 最近写出时间
         */
        protected long lastWriteTimestamp = System.currentTimeMillis();
        /**
         * 最近打印时间
         */
        protected long lastPrintTimestamp = System.currentTimeMillis();
        /**
         * 传输的偏移量
         */
        protected long transferOffset = 0;

        public AbstractWriteSocketService(final SocketChannel socketChannel) throws IOException {
            //创建 selector 注册 写事件
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.setDaemon(true);
            haWriter = new HAWriter();
            //注册钩子 记录最近读写出时间，进行流量监控
            haWriter.registerHook(writeSize -> {
                flowMonitor.addByteCountTransferred(writeSize);
                if (writeSize > 0) {
                    AbstractWriteSocketService.this.lastWriteTimestamp =
                        haService.getDefaultMessageStore().getSystemClock().now();
                }
            });
        }

        public long getNextTransferFromWhere() {
            return this.nextTransferFromWhere;
        }

        private boolean buildHandshakeBuffer() {
            //获取最后一个 EpochEntry
            final List<EpochEntry> epochEntries = AutoSwitchHAConnection.this.epochCache.getAllEntries();
            final int lastEpoch = AutoSwitchHAConnection.this.epochCache.lastEpoch();
            //最大偏移量
            final long maxPhyOffset = AutoSwitchHAConnection.this.haService.getDefaultMessageStore().getMaxPhyOffset();
            this.byteBufferHeader.position(0);
            this.byteBufferHeader.limit(MSG_HEADER_SIZE);
            //4个字节 链接当前状态的 枚举值值
            // State
            this.byteBufferHeader.putInt(currentState.ordinal());
            //4个 字节 epochEntries 大小
            // Body size
            this.byteBufferHeader.putInt(epochEntries.size() * EPOCH_ENTRY_SIZE);
            // Offset 8 字节 最大偏移量
            this.byteBufferHeader.putLong(maxPhyOffset);
            // Epoch 4个字节 最大 Epoch
            this.byteBufferHeader.putInt(lastEpoch);
            // EpochStartOffset (not needed in handshake)
            this.byteBufferHeader.putLong(0L);
            // Additional info (not needed in handshake)
            this.byteBufferHeader.putLong(0L);
            this.byteBufferHeader.flip();
            // 4 字节连接状态枚举值 + 4 字节 所有epochEntries 大小  + 8 字节的最大 偏移量 + 4个字节 最大 Epoch + 8个 字节 + 8个 字节 Epoch 开始偏移量 + 8 个字节 附加信息 确认的偏移量
            // 将 epochEntries 写入 handShakeBuffer
            // EpochEntries
            this.handShakeBuffer.position(0);
            this.handShakeBuffer.limit(EPOCH_ENTRY_SIZE * epochEntries.size());
            for (final EpochEntry entry : epochEntries) {
                if (entry != null) {
                    this.handShakeBuffer.putInt(entry.getEpoch());
                    this.handShakeBuffer.putLong(entry.getStartOffset());
                }
            }
            this.handShakeBuffer.flip();
            LOGGER.info("Master build handshake header: maxEpoch:{}, maxOffset:{}, epochEntries:{}", lastEpoch, maxPhyOffset, epochEntries);
            return true;
        }

        private boolean handshakeWithSlave() throws IOException {
            // Write Header
            // 写头信息
            boolean result = this.haWriter.write(this.socketChannel, this.byteBufferHeader);

            if (!result) {
                return false;
            }
            // 写入 body
            // Write Body
            return this.haWriter.write(this.socketChannel, this.handShakeBuffer);
        }

        // Normal transfer method
        private void buildTransferHeaderBuffer(long nextOffset, int bodySize) {
            //根据当前的 Epoch 获取 EpochEntry
            EpochEntry entry = AutoSwitchHAConnection.this.epochCache.getEntry(AutoSwitchHAConnection.this.currentTransferEpoch);

            if (entry == null) {

                // If broker is started on empty disk and no message entered (nextOffset = -1 and currentTransferEpoch = -1), do not output error log when sending heartbeat
                //如果broker 从空磁盘上启动 不 打印错误日志
                if (nextOffset != -1 || currentTransferEpoch != -1 || bodySize > 0) {
                    LOGGER.error("Failed to find epochEntry with epoch {} when build msg header", AutoSwitchHAConnection.this.currentTransferEpoch);
                }

                if (bodySize > 0) {
                    return;
                }
                // 用来做心跳传输的
                // Maybe it's used for heartbeat
                entry = AutoSwitchHAConnection.this.epochCache.firstEntry();
            }
            // Build Header
            this.byteBufferHeader.position(0);
            this.byteBufferHeader.limit(MSG_HEADER_SIZE);
            // State 4个字节 链接当前状态的 枚举值值
            this.byteBufferHeader.putInt(currentState.ordinal());
            // Body size 内容大小
            this.byteBufferHeader.putInt(bodySize);
            // Offset 下一个偏移量
            this.byteBufferHeader.putLong(nextOffset);
            // Epoch 对应的代数
            this.byteBufferHeader.putInt(entry.getEpoch());
            // EpochStartOffset 代数的开始偏移亮
            this.byteBufferHeader.putLong(entry.getStartOffset());
            // Additional info(confirm offset) 附加信息 确认的偏移量
            final long confirmOffset = AutoSwitchHAConnection.this.haService.getConfirmOffset();
            this.byteBufferHeader.putLong(confirmOffset);
            this.byteBufferHeader.flip();
        }

        /**
         * 超过间隔时间，发送心跳消息
         * @return
         * @throws Exception
         */
        private boolean sendHeartbeatIfNeeded() throws Exception {
            //间隔时间 是否需要发送心跳消息 构建发送心跳消息
            long interval = haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;
            if (interval > haService.getDefaultMessageStore().getMessageStoreConfig().getHaSendHeartbeatInterval()) {
                buildTransferHeaderBuffer(this.nextTransferFromWhere, 0);
                return this.transferData(0);
            }
            return true;
        }

        private void transferToSlave() throws Exception {
            //最近刚是否写入完成，写入完成发送心跳信息
            if (this.lastWriteOver) {
                this.lastWriteOver = sendHeartbeatIfNeeded();
            } else {
                // 如果没有写入完成，则继续进行传输
                // maxTransferSize == -1 means to continue transfer remaining data.
                this.lastWriteOver = this.transferData(-1);
            }
            //还没有传输完成 则下次继续传输
            if (!this.lastWriteOver) {
                return;
            }

            int size = this.getNextTransferDataSize();
            if (size > 0) {
                //超过 32k 以 32k 为传输
                if (size > haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                    size = haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                }
                int canTransferMaxBytes = flowMonitor.canTransferMaxByteNum();
                //传输 超过流量监控的 传输字节 记录日志 记录最近打印的时间 将大小 设为 可以传输的大小
                if (size > canTransferMaxBytes) {
                    if (System.currentTimeMillis() - lastPrintTimestamp > 1000) {
                        LOGGER.warn("Trigger HA flow control, max transfer speed {}KB/s, current speed: {}KB/s",
                            String.format("%.2f", flowMonitor.maxTransferByteInSecond() / 1024.0),
                            String.format("%.2f", flowMonitor.getTransferredByteInSecond() / 1024.0));
                        lastPrintTimestamp = System.currentTimeMillis();
                    }
                    size = canTransferMaxBytes;
                }
                //如果可以传输大小 为 0 则进行等待
                if (size <= 0) {
                    this.releaseData();
                    this.waitForRunning(100);
                    return;
                }

                // We must ensure that the transmitted logs are within the same epoch
                // If currentEpochEndOffset == -1, means that currentTransferEpoch = last epoch, so the endOffset = Long.max
                final long currentEpochEndOffset = AutoSwitchHAConnection.this.currentTransferEpochEndOffset;
                // 如果 currentEpochEndOffset 则表示 为最后一个 epoch
                // 传输位置 + 要传输的大小 超过 currentEpochEndOffset
                if (currentEpochEndOffset != -1 && this.nextTransferFromWhere + size > currentEpochEndOffset) {
                    final EpochEntry epochEntry = AutoSwitchHAConnection.this.epochCache.nextEntry(AutoSwitchHAConnection.this.currentTransferEpoch);
                    if (epochEntry == null) {
                        LOGGER.error("Can't find a bigger epochEntry than epoch {}", AutoSwitchHAConnection.this.currentTransferEpoch);
                        waitForRunning(100);
                        return;
                    }
                    //重新设置大小 为 当前 Epoch 剩余偏移量的大小
                    size = (int) (currentEpochEndOffset - this.nextTransferFromWhere);
                    //重新设置当前 Epoch 和  currentTransferEpochEndOffset
                    changeTransferEpochToNext(epochEntry);
                }
                //传输位置 进行偏移 ,下次传输位置进行偏移量
                this.transferOffset = this.nextTransferFromWhere;
                this.nextTransferFromWhere += size;
                updateLastTransferInfo();

                // Build Header
                buildTransferHeaderBuffer(this.transferOffset, size);
                //进行传输 传输头信息 传输消息 内容
                this.lastWriteOver = this.transferData(size);
            } else {
                // If size == 0, we should update the lastCatchupTimeMs
                // 如果剩余传 大小 为0 表示slave 已经追赶上 master 更新 CaughtUpTime
                AutoSwitchHAConnection.this.haService.updateConnectionLastCaughtUpTime(AutoSwitchHAConnection.this.slaveAddress, System.currentTimeMillis());
                haService.getWaitNotifyObject().allWaitForRunning(100);
            }
        }

        @Override
        public void run() {
            AutoSwitchHAConnection.LOGGER.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);

                    switch (currentState) {
                        case HANDSHAKE:
                            // Wait until the slave send it handshake msg to master.
                            // 如果状态是 HANDSHAKE 则等到 从 发送 handshake msg 给到 master
                            if (!isSlaveSendHandshake) {
                                this.waitForRunning(10);
                                continue;
                            }
                            //最近是否写完,如果写完则重新进行构建
                            if (this.lastWriteOver) {
                                if (!buildHandshakeBuffer()) {
                                    LOGGER.error("AutoSwitchHAConnection build handshake buffer failed");
                                    this.waitForRunning(5000);
                                    continue;
                                }
                            }
                            //进行传输 如果写 则将标记 改成 false 等待 slave的 通知
                            this.lastWriteOver = handshakeWithSlave();
                            if (this.lastWriteOver) {
                                // change flag to {false} to wait for slave notification
                                isSlaveSendHandshake = false;
                            }
                            break;
                        case TRANSFER:
                            //如果从请求的偏移量为-1 则进行等待 slave 获取对应的偏移量
                            if (-1 == slaveRequestOffset) {
                                this.waitForRunning(10);
                                continue;
                            }
                            //如果下次传输偏移量 为 -1
                            if (-1 == this.nextTransferFromWhere) {
                                //slaveRequestOffset 为 0 则 根据 master 的 配置 来进行传输
                                if (0 == slaveRequestOffset) {
                                    // We must ensure that the starting point of syncing log
                                    // must be the startOffset of a file (maybe the last file, or the minOffset)
                                    final MessageStoreConfig config = haService.getDefaultMessageStore().getMessageStoreConfig();
                                    //从最后一个文件开始同步 计算最后一个文件的开始偏移量 从该偏移量开始传输
                                    if (AutoSwitchHAConnection.this.isSyncFromLastFile) {
                                        long masterOffset = haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
                                        masterOffset = masterOffset - (masterOffset % config.getMappedFileSizeCommitLog());
                                        if (masterOffset < 0) {
                                            masterOffset = 0;
                                        }
                                        this.nextTransferFromWhere = masterOffset;
                                    } else {
                                        //如果不是最后一文件传输 则 从最小 偏移量开始传输
                                        this.nextTransferFromWhere = haService.getDefaultMessageStore().getCommitLog().getMinOffset();
                                    }
                                } else {
                                    // 根据 salve 请求的偏移量开始传输
                                    this.nextTransferFromWhere = slaveRequestOffset;
                                }

                                // nextTransferFromWhere is not found. It may be empty disk and no message is entered
                                //如果 nextTransferFromWhere 是 -1 则表示磁盘 是空的 进行等待 等待消息
                                if (this.nextTransferFromWhere == -1) {
                                    sendHeartbeatIfNeeded();
                                    waitForRunning(500);
                                    break;
                                }
                                // Setup initial transferEpoch
                                // 根据传输的偏移量找到 epochEntry 设置当前传输 epochEntry
                                EpochEntry epochEntry = AutoSwitchHAConnection.this.epochCache.findEpochEntryByOffset(this.nextTransferFromWhere);
                                if (epochEntry == null) {
                                    LOGGER.error("Failed to find an epochEntry to match nextTransferFromWhere {}", this.nextTransferFromWhere);
                                    sendHeartbeatIfNeeded();
                                    waitForRunning(500);
                                    break;
                                }
                                //设置 当前传输 Epoch  当前 Epoch 结束偏移量
                                changeTransferEpochToNext(epochEntry);
                                LOGGER.info("Master transfer data to slave {}, from offset:{}, currentEpoch:{}",
                                    AutoSwitchHAConnection.this.clientAddress, this.nextTransferFromWhere, epochEntry);
                            }
                            //传输 到 slave
                            transferToSlave();
                            break;
                        default:
                            throw new Exception("unexpected state " + currentState);
                    }
                } catch (Exception e) {
                    AutoSwitchHAConnection.LOGGER.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }
            //关闭释放资源
            this.onStop();
            //将连接设置成关闭
            changeCurrentState(HAConnectionState.SHUTDOWN);
            //关闭写服务
            this.makeStop();
            //读服务
            readSocketService.makeStop();
            //从haService 移除此连接
            haService.removeConnection(AutoSwitchHAConnection.this);
            //获取绑定的 SelectionKey 进行取消
            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }
            //关闭 selector, 关闭 socketChannel
            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                AutoSwitchHAConnection.LOGGER.error("", e);
            }
            AutoSwitchHAConnection.LOGGER.info(this.getServiceName() + " service end");
        }

        abstract protected int getNextTransferDataSize();

        abstract protected void releaseData();

        abstract protected boolean transferData(int maxTransferSize) throws Exception;

        abstract protected void onStop();
    }
}
