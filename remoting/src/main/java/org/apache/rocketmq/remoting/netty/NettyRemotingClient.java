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
package org.apache.rocketmq.remoting.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import java.io.IOException;
import java.net.SocketAddress;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private static final long LOCK_TIMEOUT_MILLIS = 3000;

    private final NettyClientConfig nettyClientConfig;
    private final Bootstrap bootstrap = new Bootstrap();
    /**
     * worker 线程组
     */
    private final EventLoopGroup eventLoopGroupWorker;
    /**
     * 用来 保证 channelTables 的锁
     */
    private final Lock lockChannelTables = new ReentrantLock();
    /**
     * key 为 地址 , value 为  ChannelWrapper
     */
    private final ConcurrentMap<String /* addr */, ChannelWrapper> channelTables = new ConcurrentHashMap<String, ChannelWrapper>();
    /**
     * 用来检测超时请求 和 提交检测可用的 NameServer
     */
    private final Timer timer = new Timer("ClientHouseKeepingService", true);
    /**
     * NameServer 地址集合
     */
    private final AtomicReference<List<String>> namesrvAddrList = new AtomicReference<List<String>>();
    /**
     * 可用的 NameServer
     */
    private final ConcurrentMap<String, Boolean> availableNamesrvAddrMap = new ConcurrentHashMap<String, Boolean>();
    /**
     * 选择的 NameServer
     */
    private final AtomicReference<String> namesrvAddrChoosed = new AtomicReference<String>();
    /**
     * NameServer 的 index 生成器
     */
    private final AtomicInteger namesrvIndex = new AtomicInteger(initValueIndex());
    /**
     * 用来保证 namesrvAddrChoosed 锁
     */
    private final Lock namesrvChannelLock = new ReentrantLock();
    /**
     * 用来执行回调
     * 注册对应处理器 如果没有对应 ExecutorService 则 采用 publicExecutor
     */
    private final ExecutorService publicExecutor;
    /**
     * 用来扫描 NameServer 创建对应 channel
     */
    private final ExecutorService scanExecutor;

    /**
     * 执行回调的线程  处理异步结果
     * Invoke the callback methods in this executor when process response.
     */
    private ExecutorService callbackExecutor;
    /**
     * ChannelEvent 监听者
     */
    private final ChannelEventListener channelEventListener;
    /**
     * 默认的执行线程组
     */
    private EventExecutorGroup defaultEventExecutorGroup;

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig,
        final ChannelEventListener channelEventListener) {
        this(nettyClientConfig, channelEventListener, null, null);
    }

    public NettyRemotingClient(final NettyClientConfig nettyClientConfig,
        final ChannelEventListener channelEventListener,
        final EventLoopGroup eventLoopGroup,
        final EventExecutorGroup eventExecutorGroup) {
        super(nettyClientConfig.getClientOnewaySemaphoreValue(), nettyClientConfig.getClientAsyncSemaphoreValue());
        this.nettyClientConfig = nettyClientConfig;
        this.channelEventListener = channelEventListener;

        int publicThreadNums = nettyClientConfig.getClientCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            private final AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyClientPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });

        this.scanExecutor = new ThreadPoolExecutor(4, 10, 60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(32), new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "NettyClientScan_thread_" + this.threadIndex.incrementAndGet());
                }
            }
        );

        if (eventLoopGroup != null) {
            this.eventLoopGroupWorker = eventLoopGroup;
        } else {
            this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyClientSelector_%d", this.threadIndex.incrementAndGet()));
                }
            });
        }
        this.defaultEventExecutorGroup = eventExecutorGroup;
        //TODO:: 构建 sslContext
        if (nettyClientConfig.isUseTLS()) {
            try {
                sslContext = TlsHelper.buildSslContext(true);
                LOGGER.info("SSL enabled for client");
            } catch (IOException e) {
                LOGGER.error("Failed to create SSLContext", e);
            } catch (CertificateException e) {
                LOGGER.error("Failed to create SSLContext", e);
                throw new RuntimeException("Failed to create SSLContext", e);
            }
        }
    }

    private static int initValueIndex() {
        Random r = new Random();
        return r.nextInt(999);
    }

    @Override
    public void start() {
        if (this.defaultEventExecutorGroup == null) {
            this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
                nettyClientConfig.getClientWorkerThreads(),
                new ThreadFactory() {

                    private AtomicInteger threadIndex = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "NettyClientWorkerThread_" + this.threadIndex.incrementAndGet());
                    }
                });
        }
        Bootstrap handler = this.bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, false)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.getConnectTimeoutMillis())
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    if (nettyClientConfig.isUseTLS()) {
                        if (null != sslContext) {
                            pipeline.addFirst(defaultEventExecutorGroup, "sslHandler", sslContext.newHandler(ch.alloc()));
                            LOGGER.info("Prepend SSL handler");
                        } else {
                            LOGGER.warn("Connections are insecure as SSLContext is null!");
                        }
                    }
                    ch.pipeline().addLast(
                        nettyClientConfig.isDisableNettyWorkerGroup() ? null : defaultEventExecutorGroup,
                        new NettyEncoder(),
                        new NettyDecoder(),
                        new IdleStateHandler(0, 0, nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),
                        new NettyConnectManageHandler(),
                        new NettyClientHandler());
                }
            });
        if (nettyClientConfig.getClientSocketSndBufSize() > 0) {
            LOGGER.info("client set SO_SNDBUF to {}", nettyClientConfig.getClientSocketSndBufSize());
            handler.option(ChannelOption.SO_SNDBUF, nettyClientConfig.getClientSocketSndBufSize());
        }
        if (nettyClientConfig.getClientSocketRcvBufSize() > 0) {
            LOGGER.info("client set SO_RCVBUF to {}", nettyClientConfig.getClientSocketRcvBufSize());
            handler.option(ChannelOption.SO_RCVBUF, nettyClientConfig.getClientSocketRcvBufSize());
        }
        if (nettyClientConfig.getWriteBufferLowWaterMark() > 0 && nettyClientConfig.getWriteBufferHighWaterMark() > 0) {
            LOGGER.info("client set netty WRITE_BUFFER_WATER_MARK to {},{}",
                    nettyClientConfig.getWriteBufferLowWaterMark(), nettyClientConfig.getWriteBufferHighWaterMark());
            handler.option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
                    nettyClientConfig.getWriteBufferLowWaterMark(), nettyClientConfig.getWriteBufferHighWaterMark()));
        }
        if (nettyClientConfig.isClientPooledByteBufAllocatorEnable()) {
            handler.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }
        //定时扫描超时的请求 执行回调
        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    NettyRemotingClient.this.scanResponseTable();
                } catch (Throwable e) {
                    LOGGER.error("scanResponseTable exception", e);
                }
            }
        }, 1000 * 3, 1000);

//        this.timer.scheduleAtFixedRate(new TimerTask() {
//            @Override
//            public void run() {
//                try {
//                    NettyRemotingClient.this.scanChannelTablesOfNameServer();
//                } catch (Exception e) {
//                    LOGGER.error("scanChannelTablesOfNameServer exception", e);
//                }
//            }
//        }, 1000 * 3, 10 * 1000);
        //定时扫描可以 nameServer 地址
        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    NettyRemotingClient.this.scanAvailableNameSrv();
                } catch (Exception e) {
                    LOGGER.error("scanAvailableNameSrv exception", e);
                }
            }
        }, 0, this.nettyClientConfig.getConnectTimeoutMillis());

    }

    @Override
    public void shutdown() {
        try {
            //取消扫描 可用的 Nameserver 和 超时的请求
            this.timer.cancel();
            //关闭已经 建立的连接
            for (String addr : this.channelTables.keySet()) {
                this.closeChannel(addr, this.channelTables.get(addr).getChannel());
            }

            this.channelTables.clear();
            //关闭Worker 线程
            this.eventLoopGroupWorker.shutdownGracefully();
            //关闭 执行 ChannelEventListener 线程池
            if (this.nettyEventExecutor != null) {
                this.nettyEventExecutor.shutdown();
            }
            //关闭 执行线程
            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            LOGGER.error("NettyRemotingClient shutdown exception, ", e);
        }
        //关闭公共线程池
        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                LOGGER.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
        //关闭扫描可用NameServer 线程池
        if (this.scanExecutor != null) {
            try {
                this.scanExecutor.shutdown();
            } catch (Exception e) {
                LOGGER.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }

    /**
     * 关闭 channel
     * @param addr
     * @param channel
     */
    public void closeChannel(final String addr, final Channel channel) {
        if (null == channel) {
            return;
        }

        final String addrRemote = null == addr ? RemotingHelper.parseChannelRemoteAddr(channel) : addr;

        try {
            //尝试上锁
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    final ChannelWrapper prevCW = this.channelTables.get(addrRemote);

                    LOGGER.info("closeChannel: begin close the channel[{}] Found: {}", addrRemote, prevCW != null);
                    //channel 已经被移除
                    if (null == prevCW) {
                        LOGGER.info("closeChannel: the channel[{}] has been removed from the channel table before", addrRemote);
                        removeItemFromTable = false;
                    }
                    //旧channel 已经关闭 ,但是被由创建
                    else if (prevCW.getChannel() != channel) {
                        LOGGER.info("closeChannel: the channel[{}] has been closed before, and has been created again, nothing to do.",
                            addrRemote);
                        removeItemFromTable = false;
                    }
                    //进行移除
                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        LOGGER.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                    }
                    //关闭channel
                    RemotingUtil.closeChannel(channel);
                } catch (Exception e) {
                    LOGGER.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                LOGGER.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            LOGGER.error("closeChannel exception", e);
        }
    }

    public void closeChannel(final Channel channel) {
        if (null == channel) {
            return;
        }

        try {
            //尝试上锁 从channelTables 找到对应的 ChannelWrapper 和 地址 进行移除 关闭channel
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    ChannelWrapper prevCW = null;
                    String addrRemote = null;
                    for (Map.Entry<String, ChannelWrapper> entry : channelTables.entrySet()) {
                        String key = entry.getKey();
                        ChannelWrapper prev = entry.getValue();
                        if (prev.getChannel() != null) {
                            if (prev.getChannel() == channel) {
                                prevCW = prev;
                                addrRemote = key;
                                break;
                            }
                        }
                    }

                    if (null == prevCW) {
                        LOGGER.info("eventCloseChannel: the channel[{}] has been removed from the channel table before", addrRemote);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        LOGGER.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                        RemotingUtil.closeChannel(channel);
                    }
                } catch (Exception e) {
                    LOGGER.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                LOGGER.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            LOGGER.error("closeChannel exception", e);
        }
    }

    /**
     * 更新 NameServer 地址 如果已经选择的 NameServer 不在 新的地址里面 则关闭该已经选择的 NameServer
     * @param addrs
     */
    @Override
    public void updateNameServerAddressList(List<String> addrs) {
        List<String> old = this.namesrvAddrList.get();
        boolean update = false;

        if (!addrs.isEmpty()) {
            //如果旧的 列表 为空 或者 旧的和新数量 不一致 或 旧的不包含 新的 地址 需要 更新
            if (null == old) {
                update = true;
            } else if (addrs.size() != old.size()) {
                update = true;
            } else {
                for (int i = 0; i < addrs.size() && !update; i++) {
                    if (!old.contains(addrs.get(i))) {
                        update = true;
                    }
                }
            }

            if (update) {
                Collections.shuffle(addrs);
                LOGGER.info("name server address updated. NEW : {} , OLD: {}", addrs, old);
                this.namesrvAddrList.set(addrs);

                // should close the channel if choosed addr is not exist.
                //如果选择 NameServer 不在新的列表当中 进行关闭
                if (this.namesrvAddrChoosed.get() != null && !addrs.contains(this.namesrvAddrChoosed.get())) {
                    String namesrvAddr = this.namesrvAddrChoosed.get();
                    for (String addr : this.channelTables.keySet()) {
                        if (addr.contains(namesrvAddr)) {
                            ChannelWrapper channelWrapper = this.channelTables.get(addr);
                            if (channelWrapper != null) {
                                closeChannel(channelWrapper.getChannel());
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * 执同步调用
     * @param addr
     * @param request
     * @param timeoutMillis
     * @return
     * @throws InterruptedException
     * @throws RemotingConnectException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     */
    @Override
    public RemotingCommand invokeSync(String addr, final RemotingCommand request, long timeoutMillis)
        throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException {
        long beginStartTime = System.currentTimeMillis();
        //根据地址获取 对应的 channel
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                //执行请求前钩子
                doBeforeRpcHooks(addr, request);
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTime) {
                    throw new RemotingTimeoutException("invokeSync call the addr[" + addr + "] timeout");
                }
                RemotingCommand response = this.invokeSyncImpl(channel, request, timeoutMillis - costTime);
                //执行请求后钩子
                doAfterRpcHooks(RemotingHelper.parseChannelRemoteAddr(channel), request, response);
                this.updateChannelLastResponseTime(addr);
                return response;
            } catch (RemotingSendRequestException e) {
                LOGGER.warn("invokeSync: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            } catch (RemotingTimeoutException e) {
                if (nettyClientConfig.isClientCloseSocketIfTimeout()) {
                    this.closeChannel(addr, channel);
                    LOGGER.warn("invokeSync: close socket because of timeout, {}ms, {}", timeoutMillis, addr);
                }
                LOGGER.warn("invokeSync: wait response timeout exception, the channel[{}]", addr);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    @Override
    public void closeChannels(List<String> addrList) {
        for (String addr : addrList) {
            ChannelWrapper cw = this.channelTables.get(addr);
            if (cw == null) {
                continue;
            }
            this.closeChannel(addr, cw.getChannel());
        }
        interruptPullRequests(new HashSet<String>(addrList));
    }

    private void interruptPullRequests(Set<String> brokerAddrSet) {
        for (ResponseFuture responseFuture : responseTable.values()) {
            RemotingCommand cmd = responseFuture.getRequestCommand();
            if (cmd == null) {
                continue;
            }
            String remoteAddr = RemotingHelper.parseChannelRemoteAddr(responseFuture.getChannel());
            // interrupt only pull message request
            if (brokerAddrSet.contains(remoteAddr) && (cmd.getCode() == 11 || cmd.getCode() == 361)) {
                LOGGER.info("interrupt {}", cmd);
                responseFuture.interrupt();
            }
        }
    }

    /**
     * 根据地址 修改 channel 最近 的响应时间
     * @param addr
     */
    private void updateChannelLastResponseTime(final String addr) {
        String address = addr;
        if (address == null) {
            address = this.namesrvAddrChoosed.get();
        }
        if (address == null) {
            LOGGER.warn("[updateChannelLastResponseTime] could not find address!!");
            return;
        }
        ChannelWrapper channelWrapper = this.channelTables.get(address);
        if (channelWrapper != null && channelWrapper.isOK()) {
            channelWrapper.updateLastResponseTime();
        }
    }

    /**
     * 根据地址获取 channel 不存在 生成新的连接
     * @param addr
     * @return
     * @throws InterruptedException
     */
    private Channel getAndCreateChannel(final String addr) throws InterruptedException {
        //地址为 null 直接从 namesrvAddrChoosed 获取 Channel
        if (null == addr) {
            return getAndCreateNameserverChannel();
        }

        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }
        //创建新的连接
        return this.createChannel(addr);
    }

    /**
     * 获取 nameServer 的 channel
     * @return
     * @throws InterruptedException
     */
    private Channel getAndCreateNameserverChannel() throws InterruptedException {
        //快速从 namesrvAddrChoosed 获取 Channel
        String addr = this.namesrvAddrChoosed.get();
        if (addr != null) {
            ChannelWrapper cw = this.channelTables.get(addr);
            if (cw != null && cw.isOK()) {
                return cw.getChannel();
            }
        }

        final List<String> addrList = this.namesrvAddrList.get();
        //尝试获取锁进行上锁
        if (this.namesrvChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            //快速从 namesrvAddrChoosed 获取 Channel
            try {
                addr = this.namesrvAddrChoosed.get();
                if (addr != null) {
                    ChannelWrapper cw = this.channelTables.get(addr);
                    if (cw != null && cw.isOK()) {
                        return cw.getChannel();
                    }
                }
            //生成一个随机数,然后从这个随机数之后递增 从addrList 当中获取 一个地址 生成 对应的 Channel
                if (addrList != null && !addrList.isEmpty()) {
                    for (int i = 0; i < addrList.size(); i++) {
                        int index = this.namesrvIndex.incrementAndGet();
                        index = Math.abs(index);
                        index = index % addrList.size();
                        String newAddr = addrList.get(index);

                        this.namesrvAddrChoosed.set(newAddr);
                        LOGGER.info("new name server is chosen. OLD: {} , NEW: {}. namesrvIndex = {}", addr, newAddr, namesrvIndex);
                        Channel channelNew = this.createChannel(newAddr);
                        if (channelNew != null) {
                            return channelNew;
                        }
                    }
                    throw new RemotingConnectException(addrList.toString());
                }
            } catch (Exception e) {
                LOGGER.error("getAndCreateNameserverChannel: create name server channel exception", e);
            } finally {
                this.namesrvChannelLock.unlock();
            }
        } else {
            LOGGER.warn("getAndCreateNameserverChannel: try to lock name server, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        return null;
    }

    private Channel createChannel(final String addr) throws InterruptedException {
        //快速根据地址 查找是是否已经 有对应的 Channel
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }
        //尝试获取锁进行上锁
        if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                boolean createNewConnection;
                //由于并发可能导致已经有对应的 channel
                cw = this.channelTables.get(addr);
                if (cw != null) {

                    if (cw.isOK()) {
                        return cw.getChannel();
                        //FIXME::cw.getChannelFuture().isDone()?
                    } else if (!cw.getChannelFuture().isDone()) {
                        createNewConnection = false;
                    } else {
                        //进行移除 创建新的连接
                        this.channelTables.remove(addr);
                        createNewConnection = true;
                    }
                } else {
                    createNewConnection = true;
                }
                //需要创建新的连接 创建新的连接 放置到 channelTables
                if (createNewConnection) {
                    ChannelFuture channelFuture = this.bootstrap.connect(RemotingHelper.string2SocketAddress(addr));
                    LOGGER.info("createChannel: begin to connect remote host[{}] asynchronously", addr);
                    cw = new ChannelWrapper(channelFuture);
                    this.channelTables.put(addr, cw);
                }
            } catch (Exception e) {
                LOGGER.error("createChannel: create channel exception", e);
            } finally {
                this.lockChannelTables.unlock();
            }
        } else {
            LOGGER.warn("createChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        if (cw != null) {
            ChannelFuture channelFuture = cw.getChannelFuture();
            //等待连接超时 如果channel 是 好的 则进行 返回
            if (channelFuture.awaitUninterruptibly(this.nettyClientConfig.getConnectTimeoutMillis())) {
                if (cw.isOK()) {
                    LOGGER.info("createChannel: connect remote host[{}] success, {}", addr, channelFuture.toString());
                    return cw.getChannel();
                } else {
                    LOGGER.warn("createChannel: connect remote host[" + addr + "] failed, " + channelFuture.toString());
                }
            } else {
                LOGGER.warn("createChannel: connect remote host[{}] timeout {}ms, {}", addr, this.nettyClientConfig.getConnectTimeoutMillis(),
                    channelFuture.toString());
            }
        }

        return null;
    }

    /**
     * 异步请求调用
     * @param addr
     * @param request
     * @param timeoutMillis
     * @param invokeCallback
     * @throws InterruptedException
     * @throws RemotingConnectException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    @Override
    public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
        throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException,
        RemotingSendRequestException {
        //计算开始时间
        long beginStartTime = System.currentTimeMillis();
        //根据地址 获取 channel
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                //在发送RPC请求之前 执行钩子
                doBeforeRpcHooks(addr, request);
                //计算到现在已经花费的时间如果 已经超时返回超时异常
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTime) {
                    throw new RemotingTooMuchRequestException("invokeAsync call the addr[" + addr + "] timeout");
                }
                this.invokeAsyncImpl(channel, request, timeoutMillis - costTime, new InvokeCallbackWrapper(invokeCallback, addr));
            } catch (RemotingSendRequestException e) {
                LOGGER.warn("invokeAsync: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    /**
     * 单向调用
     * @param addr
     * @param request
     * @param timeoutMillis
     * @throws InterruptedException
     * @throws RemotingConnectException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    @Override
    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws InterruptedException,
        RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        //根据地址 获取 channel
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                //在发送RPC请求之前 执行钩子
                doBeforeRpcHooks(addr, request);
                this.invokeOnewayImpl(channel, request, timeoutMillis);
            } catch (RemotingSendRequestException e) {
                LOGGER.warn("invokeOneway: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new RemotingConnectException(addr);
        }
    }

    /**
     * 注册对应处理器 如果没有对应 ExecutorService 则 采用 publicExecutor
     * @param requestCode
     * @param processor
     * @param executor
     */
    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        ExecutorService executorThis = executor;
        if (null == executor) {
            executorThis = this.publicExecutor;
        }

        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<NettyRequestProcessor, ExecutorService>(processor, executorThis);
        this.processorTable.put(requestCode, pair);
    }

    @Override
    public boolean isChannelWritable(String addr) {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.isWritable();
        }
        return true;
    }

    @Override
    public List<String> getNameServerAddressList() {
        return this.namesrvAddrList.get();
    }

    @Override
    public List<String> getAvailableNameSrvList() {
        return new ArrayList<String>(this.availableNamesrvAddrMap.keySet());
    }

    @Override
    public ChannelEventListener getChannelEventListener() {
        return channelEventListener;
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        if (nettyClientConfig.isDisableCallbackExecutor()) {
            return null;
        }
        return callbackExecutor != null ? callbackExecutor : publicExecutor;
    }

    @Override
    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.callbackExecutor = callbackExecutor;
    }

    /**
     * 扫描 NameSever列表如果 发现 channel 最近的更新时间  已经 超时 就关闭 channel
     */
    protected void scanChannelTablesOfNameServer() {
        List<String> nameServerList = this.namesrvAddrList.get();
        if (nameServerList == null) {
            LOGGER.warn("[SCAN] Addresses of name server is empty!");
            return;
        }

        for (String addr : this.channelTables.keySet()) {
            ChannelWrapper channelWrapper = this.channelTables.get(addr);
            if (channelWrapper == null) {
                continue;
            }

            if ((System.currentTimeMillis() - channelWrapper.getLastResponseTime()) > this.nettyClientConfig.getChannelNotActiveInterval()) {
                LOGGER.warn("[SCAN] No response after {} from name server {}, so close it!", channelWrapper.getLastResponseTime(),
                    addr);
                closeChannel(addr, channelWrapper.getChannel());
            }
        }
    }

    /**
     * 扫描可以 nameServer 地址
     */
    private void scanAvailableNameSrv() {
        List<String> nameServerList = this.namesrvAddrList.get();
        if (nameServerList == null) {
            LOGGER.debug("scanAvailableNameSrv Addresses of name server is empty!");
            return;
        }

        for (final String namesrvAddr : nameServerList) {
            scanExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        //创建新的 Channel
                        Channel channel = NettyRemotingClient.this.getAndCreateChannel(namesrvAddr);
                        if (channel != null) {
                            NettyRemotingClient.this.availableNamesrvAddrMap.putIfAbsent(namesrvAddr, true);
                        } else {
                            NettyRemotingClient.this.availableNamesrvAddrMap.remove(namesrvAddr);
                        }
                    } catch (Exception e) {
                        LOGGER.error("scanAvailableNameSrv get channel of {} failed, ", namesrvAddr, e);
                    }
                }
            });
        }

    }

    static class ChannelWrapper {
        private final ChannelFuture channelFuture;
        // only affected by sync or async request, oneway is not included.

        //上次回应的时间
        private long lastResponseTime;

        public ChannelWrapper(ChannelFuture channelFuture) {
            this.channelFuture = channelFuture;
            this.lastResponseTime = System.currentTimeMillis();
        }

        public boolean isOK() {
            return this.channelFuture.channel() != null && this.channelFuture.channel().isActive();
        }

        public boolean isWritable() {
            return this.channelFuture.channel().isWritable();
        }

        private Channel getChannel() {
            return this.channelFuture.channel();
        }

        public ChannelFuture getChannelFuture() {
            return channelFuture;
        }

        public long getLastResponseTime() {
            return this.lastResponseTime;
        }

        public void updateLastResponseTime() {
            this.lastResponseTime = System.currentTimeMillis();
        }
    }

    class InvokeCallbackWrapper implements InvokeCallback {

        private final InvokeCallback invokeCallback;
        private final String addr;

        public InvokeCallbackWrapper(InvokeCallback invokeCallback, String addr) {
            this.invokeCallback = invokeCallback;
            this.addr = addr;
        }

        @Override
        public void operationComplete(ResponseFuture responseFuture) {
            if (responseFuture != null && responseFuture.isSendRequestOK() && responseFuture.getResponseCommand() != null) {
                NettyRemotingClient.this.updateChannelLastResponseTime(addr);
            }
            this.invokeCallback.operationComplete(responseFuture);
        }
    }

    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

    class NettyConnectManageHandler extends ChannelDuplexHandler {
        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
            ChannelPromise promise) throws Exception {
            final String local = localAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(localAddress);
            final String remote = remoteAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(remoteAddress);
            LOGGER.info("NETTY CLIENT PIPELINE: CONNECT  {} => {}", local, remote);

            super.connect(ctx, remoteAddress, localAddress, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remote, ctx.channel()));
            }
        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            LOGGER.info("NETTY CLIENT PIPELINE: DISCONNECT {}", remoteAddress);
            closeChannel(ctx.channel());
            super.disconnect(ctx, promise);

            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            LOGGER.info("NETTY CLIENT PIPELINE: CLOSE {}", remoteAddress);
            closeChannel(ctx.channel());
            super.close(ctx, promise);
            NettyRemotingClient.this.failFast(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress, ctx.channel()));
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    LOGGER.warn("NETTY CLIENT PIPELINE: IDLE exception [{}]", remoteAddress);
                    closeChannel(ctx.channel());
                    if (NettyRemotingClient.this.channelEventListener != null) {
                        NettyRemotingClient.this
                            .putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress, ctx.channel()));
                    }
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            LOGGER.warn("NETTY CLIENT PIPELINE: exceptionCaught {}", remoteAddress);
            LOGGER.warn("NETTY CLIENT PIPELINE: exceptionCaught exception.", cause);
            closeChannel(ctx.channel());
            if (NettyRemotingClient.this.channelEventListener != null) {
                NettyRemotingClient.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress, ctx.channel()));
            }
        }
    }
}
