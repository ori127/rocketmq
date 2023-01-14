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
package org.apache.rocketmq.client.impl.factory;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.admin.MQAdminExtInner;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.ClientRemotingProcessor;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQAdminImpl;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.MQConsumerInner;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.impl.consumer.PullMessageService;
import org.apache.rocketmq.client.impl.consumer.RebalanceService;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.MQProducerInner;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueAssignment;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.rocketmq.common.rpc.ClientMetadata.topicRouteData2EndpointsForStaticTopic;

public class MQClientInstance {
    /**
     * 锁超时时间
     */
    private final static long LOCK_TIMEOUT_MILLIS = 3000;
    private final static InternalLogger log = ClientLogger.getLog();
    /**
     * 客户端配置
     */
    private final ClientConfig clientConfig;
    /**
     * 客户端id
     */
    private final String clientId;
    /**
     * 引导时间戳
     */
    private final long bootTimestamp = System.currentTimeMillis();

    /**
     * key 为 生产组名,value 为对应的生产者 生产组 和 生产 者实例的映射 关系
     * The container of the producer in the current client. The key is the name of producerGroup.
     */
    private final ConcurrentMap<String, MQProducerInner> producerTable = new ConcurrentHashMap<>();

    /**
     * key 为 消息组名,value 为对应的消费者 消费组 和 消费 者实例的映射 关系
     * The container of the consumer in the current client. The key is the name of consumerGroup.
     */
    private final ConcurrentMap<String, MQConsumerInner> consumerTable = new ConcurrentHashMap<>();

    /**
     * The container of the adminExt in the current client. The key is the name of adminExtGroup.
     */
    private final ConcurrentMap<String, MQAdminExtInner> adminExtTable = new ConcurrentHashMap<>();
    /**
     * netty客户端配置
     */
    private final NettyClientConfig nettyClientConfig;
    private final MQClientAPIImpl mQClientAPIImpl;
    private final MQAdminImpl mQAdminImpl;
    /**
     * key 为 topic , value 为 TopicRouteData
     */
    private final ConcurrentMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<>();
    /**
     * key 为topic , value.key 为 MessageQueue , value.value 为 brokerName
     */
    private final ConcurrentMap<String/* Topic */, ConcurrentMap<MessageQueue, String/*brokerName*/>> topicEndPointsTable = new ConcurrentHashMap<>();
    /**
     * Nameserver 的锁
     */
    private final Lock lockNamesrv = new ReentrantLock();
    /**
     * 用于心跳的锁
     */
    private final Lock lockHeartbeat = new ReentrantLock();

    /**
     * The container which stores the brokerClusterInfo. The key of the map is the brokerCluster name.
     * And the value is the broker instance list that belongs to the broker cluster.
     * For the sub map, the key is the id of single broker instance, and the value is the address.
     * key 为 brokerName , value.key  为 brokerId , value.value 为 broker 地址 brokerName 和  broker 映射关系
     */
    private final ConcurrentMap<String, HashMap<Long, String>> brokerAddrTable = new ConcurrentHashMap<>();
    /**
     * key 为 brokerName , value.key  为 broker 地址  , value.value 为 broker 的版本信息 brokerName 和  broker 映射关系
     */
    private final ConcurrentMap<String/* Broker Name */, HashMap<String/* address */, Integer>> brokerVersionTable = new ConcurrentHashMap<>();
    /**
     * 定时 进行延迟 delayMillis 毫秒 唤醒
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "MQClientFactoryScheduledThread"));
    /**
     * 根据获取 消息请求 获取消息 然后 consumeMessageService 进行消费
     */
    private final PullMessageService pullMessageService;
    /**
     * 负载均衡服务进行负载均衡
     */
    private final RebalanceService rebalanceService;
    /**
     * 生产者
     */
    private final DefaultMQProducer defaultMQProducer;
    /**
     * 消费者状态管理 用来 统计 消费的信息
     */
    private final ConsumerStatsManager consumerStatsManager;
    /**
     * 发送心跳的总计数
     */
    private final AtomicLong sendHeartbeatTimesTotal = new AtomicLong(0);
    /**
     * 服务状态
     */
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private final Random random = new Random();

    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId) {
        this(clientConfig, instanceIndex, clientId, null);
    }

    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId, RPCHook rpcHook) {
        this.clientConfig = clientConfig;
        this.nettyClientConfig = new NettyClientConfig();
        this.nettyClientConfig.setClientCallbackExecutorThreads(clientConfig.getClientCallbackExecutorThreads());
        this.nettyClientConfig.setUseTLS(clientConfig.isUseTLS());
        ClientRemotingProcessor clientRemotingProcessor = new ClientRemotingProcessor(this);
        this.mQClientAPIImpl = new MQClientAPIImpl(this.nettyClientConfig, clientRemotingProcessor, rpcHook, clientConfig);

        if (this.clientConfig.getNamesrvAddr() != null) {
            this.mQClientAPIImpl.updateNameServerAddressList(this.clientConfig.getNamesrvAddr());
            log.info("user specified name server address: {}", this.clientConfig.getNamesrvAddr());
        }

        this.clientId = clientId;

        this.mQAdminImpl = new MQAdminImpl(this);

        this.pullMessageService = new PullMessageService(this);

        this.rebalanceService = new RebalanceService(this);

        this.defaultMQProducer = new DefaultMQProducer(MixAll.CLIENT_INNER_PRODUCER_GROUP);
        this.defaultMQProducer.resetClientConfig(clientConfig);

        this.consumerStatsManager = new ConsumerStatsManager(this.scheduledExecutorService);

        log.info("Created a new client Instance, InstanceIndex:{}, ClientID:{}, ClientConfig:{}, ClientVersion:{}, SerializerType:{}",
                instanceIndex,
            this.clientId,
            this.clientConfig,
            MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION), RemotingCommand.getSerializeTypeConfigInThisServer());
    }

    public static TopicPublishInfo topicRouteData2TopicPublishInfo(final String topic, final TopicRouteData route) {
        TopicPublishInfo info = new TopicPublishInfo();
        // TO DO should check the usage of raw route, it is better to remove such field
        info.setTopicRouteData(route);
        //若果路由信息 包含顺序 将orderTopicConf 解析成 MessageQueue
        if (route.getOrderTopicConf() != null && route.getOrderTopicConf().length() > 0) {
            String[] brokers = route.getOrderTopicConf().split(";");
            for (String broker : brokers) {
                String[] item = broker.split(":");
                int nums = Integer.parseInt(item[1]);
                for (int i = 0; i < nums; i++) {
                    MessageQueue mq = new MessageQueue(topic, item[0], i);
                    info.getMessageQueueList().add(mq);
                }
            }

            info.setOrderTopic(true);
        } else if (route.getOrderTopicConf() == null
                && route.getTopicQueueMappingByBroker() != null
                && !route.getTopicQueueMappingByBroker().isEmpty()) {
            //不是顺序topic 将TopicRouteData 转成 MessageQueue => brokerName 的映射
            //添加到 MessageQueue 按 队列id 进行排序
            info.setOrderTopic(false);
            ConcurrentMap<MessageQueue, String> mqEndPoints = topicRouteData2EndpointsForStaticTopic(topic, route);
            info.getMessageQueueList().addAll(mqEndPoints.keySet());
            info.getMessageQueueList().sort((mq1, mq2) -> MixAll.compareInteger(mq1.getQueueId(), mq2.getQueueId()));
        } else {
            List<QueueData> qds = route.getQueueDatas();
            //根据brokerName 进行排序
            Collections.sort(qds);
            //遍历 QueueData 获取 该topic 下 broker 的队列  其 可写的队列
            for (QueueData qd : qds) {
                if (PermName.isWriteable(qd.getPerm())) {
                    BrokerData brokerData = null;
                    for (BrokerData bd : route.getBrokerDatas()) {
                        if (bd.getBrokerName().equals(qd.getBrokerName())) {
                            brokerData = bd;
                            break;
                        }
                    }

                    if (null == brokerData) {
                        continue;
                    }
                    //没有主进行跳过
                    if (!brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID)) {
                        continue;
                    }
                    //根据其可写队列数 构建 MessageQueue
                    for (int i = 0; i < qd.getWriteQueueNums(); i++) {
                        MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                        info.getMessageQueueList().add(mq);
                    }
                }
            }

            info.setOrderTopic(false);
        }

        return info;
    }

    public static Set<MessageQueue> topicRouteData2TopicSubscribeInfo(final String topic, final TopicRouteData route) {
        Set<MessageQueue> mqList = new HashSet<>();
        //FIXME::生成这两种的 MessageQueue 有什么区别?
        if (route.getTopicQueueMappingByBroker() != null
                && !route.getTopicQueueMappingByBroker().isEmpty()) {
            //将TopicRouteData 转成 MessageQueue => brokerName的 映射
            ConcurrentMap<MessageQueue, String> mqEndPoints = topicRouteData2EndpointsForStaticTopic(topic, route);
            return mqEndPoints.keySet();
        }
        //遍历路由当中 QueueData 如果 QueueData 可读 添加 qds
        List<QueueData> qds = route.getQueueDatas();
        for (QueueData qd : qds) {
            if (PermName.isReadable(qd.getPerm())) {
                for (int i = 0; i < qd.getReadQueueNums(); i++) {
                    MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                    mqList.add(mq);
                }
            }
        }

        return mqList;
    }

    public void start() throws MQClientException {

        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    //先将状态 改成 START_FAILED
                    this.serviceState = ServiceState.START_FAILED;
                    // If not specified,looking address from name server
                    //如果没有nameServer 信息 则获取nameServer地址
                    if (null == this.clientConfig.getNamesrvAddr()) {
                        this.mQClientAPIImpl.fetchNameServerAddr();
                    }
                    // Start request-response channel
                    //启动对应的客户端
                    this.mQClientAPIImpl.start();
                    // Start various schedule tasks
                    //开启各种定时任务
                    this.startScheduledTask();
                    // Start pull service
                    //启动获取消息服务 从 messageRequestQueue take 获取消息
                    this.pullMessageService.start();
                    // Start rebalance service
                    //启动负载均衡服务
                    this.rebalanceService.start();
                    // Start push service
                    this.defaultMQProducer.getDefaultMQProducerImpl().start(false);
                    log.info("the client factory [{}] start OK", this.clientId);
                    this.serviceState = ServiceState.RUNNING;
                    break;
                case START_FAILED:
                    throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
                default:
                    break;
            }
        }
    }

    private void startScheduledTask() {
        //每隔2分钟拉取 NameServer地址
        if (null == this.clientConfig.getNamesrvAddr()) {
            this.scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    MQClientInstance.this.mQClientAPIImpl.fetchNameServerAddr();
                } catch (Exception e) {
                    log.error("ScheduledTask fetchNameServerAddr exception", e);
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        }
        //每隔30秒获取 topic 的路由信息
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                MQClientInstance.this.updateTopicRouteInfoFromNameServer();
            } catch (Exception e) {
                log.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e);
            }
        }, 10, this.clientConfig.getPollNameServerInterval(), TimeUnit.MILLISECONDS);
        //每隔30秒发送心跳信息 更新 broker集群 当中的 broker信息
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                MQClientInstance.this.cleanOfflineBroker();
                MQClientInstance.this.sendHeartbeatToAllBrokerWithLock();
            } catch (Exception e) {
                log.error("ScheduledTask sendHeartbeatToAllBroker exception", e);
            }
        }, 1000, this.clientConfig.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS);
        //TODO::遍历消费者
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                MQClientInstance.this.persistAllConsumerOffset();
            } catch (Exception e) {
                log.error("ScheduledTask persistAllConsumerOffset exception", e);
            }
        }, 1000 * 10, this.clientConfig.getPersistConsumerOffsetInterval(), TimeUnit.MILLISECONDS);
        //TODO::遍历消费者集合
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                MQClientInstance.this.adjustThreadPool();
            } catch (Exception e) {
                log.error("ScheduledTask adjustThreadPool exception", e);
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    public String getClientId() {
        return clientId;
    }

    public void updateTopicRouteInfoFromNameServer() {
        Set<String> topicList = new HashSet<>();

        // Consumer
        {
            for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    Set<SubscriptionData> subList = impl.subscriptions();
                    if (subList != null) {
                        for (SubscriptionData subData : subList) {
                            topicList.add(subData.getTopic());
                        }
                    }
                }
            }
        }

        // Producer
        {
            for (Entry<String, MQProducerInner> entry : this.producerTable.entrySet()) {
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    Set<String> lst = impl.getPublishTopicList();
                    topicList.addAll(lst);
                }
            }
        }

        for (String topic : topicList) {
            this.updateTopicRouteInfoFromNameServer(topic);
        }
    }

    public Map<MessageQueue, Long> parseOffsetTableFromBroker(Map<MessageQueue, Long> offsetTable, String namespace) {
        HashMap<MessageQueue, Long> newOffsetTable = new HashMap<>(offsetTable.size(), 1);
        if (StringUtils.isNotEmpty(namespace)) {
            for (Entry<MessageQueue, Long> entry : offsetTable.entrySet()) {
                MessageQueue queue = entry.getKey();
                queue.setTopic(NamespaceUtil.withoutNamespace(queue.getTopic(), namespace));
                newOffsetTable.put(queue, entry.getValue());
            }
        } else {
            newOffsetTable.putAll(offsetTable);
        }

        return newOffsetTable;
    }

    /**
     * 清理离线的broker
     * Remove offline broker
     */
    private void cleanOfflineBroker() {
        try {
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                try {
                    ConcurrentHashMap<String, HashMap<Long, String>> updatedTable = new ConcurrentHashMap<>(this.brokerAddrTable.size(), 1);
                    //遍历 brokerAddrTable
                    Iterator<Entry<String, HashMap<Long, String>>> itBrokerTable = this.brokerAddrTable.entrySet().iterator();
                    while (itBrokerTable.hasNext()) {
                        Entry<String, HashMap<Long, String>> entry = itBrokerTable.next();
                        String brokerName = entry.getKey();
                        HashMap<Long, String> oneTable = entry.getValue();

                        HashMap<Long, String> cloneAddrTable = new HashMap<>(oneTable.size(), 1);
                        cloneAddrTable.putAll(oneTable);
                        //遍历每个 broker集群 当中 的 broker集合
                        Iterator<Entry<Long, String>> it = cloneAddrTable.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Long, String> ee = it.next();
                            String addr = ee.getValue();
                            // 遍历 topicRouteTable 中的 BrokerData 判断该地址 是否在 topicRouteTable 存在
                            //不存在 则进行 移除
                            if (!this.isBrokerAddrExistInTopicRouteTable(addr)) {
                                it.remove();
                                log.info("the broker addr[{} {}] is offline, remove it", brokerName, addr);
                            }
                        }
                        //如果移除过后cloneAddrTable 为空 则移除该  broker集群
                        if (cloneAddrTable.isEmpty()) {
                            itBrokerTable.remove();
                            log.info("the broker[{}] name's host is offline, remove it", brokerName);
                        } else {
                            //需要更新新该 broker集群 中的 broker 列表
                            updatedTable.put(brokerName, cloneAddrTable);
                        }
                    }
                    //进行更新
                    if (!updatedTable.isEmpty()) {
                        this.brokerAddrTable.putAll(updatedTable);
                    }
                } finally {
                    this.lockNamesrv.unlock();
                }
        } catch (InterruptedException e) {
            log.warn("cleanOfflineBroker Exception", e);
        }
    }

    public void checkClientInBroker() throws MQClientException {
        //遍历消费者列表的 订阅列表 跳过订阅 Tag的过滤 检查该 client 是否在 该 broker 中
        for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            Set<SubscriptionData> subscriptionInner = entry.getValue().subscriptions();
            if (subscriptionInner == null || subscriptionInner.isEmpty()) {
                return;
            }

            for (SubscriptionData subscriptionData : subscriptionInner) {
                if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                    continue;
                }
                // may need to check one broker every cluster...
                // assume that the configs of every broker in cluster are the the same.
                String addr = findBrokerAddrByTopic(subscriptionData.getTopic());

                if (addr != null) {
                    try {
                        this.getMQClientAPIImpl().checkClientInBroker(
                                addr, entry.getKey(), this.clientId, subscriptionData, clientConfig.getMqClientApiTimeout()
                        );
                    } catch (Exception e) {
                        if (e instanceof MQClientException) {
                            throw (MQClientException) e;
                        } else {
                            throw new MQClientException("Check client in broker error, maybe because you use "
                                    + subscriptionData.getExpressionType() + " to filter message, but server has not been upgraded to support!"
                                    + "This error would not affect the launch of consumer, but may has impact on message receiving if you " +
                                    "have use the new features which are not supported by server, please check the log!", e);
                        }
                    }
                }
            }
        }
    }

    /**
     * 发送心跳数据 和 注册消息 过滤 class
     */
    public void sendHeartbeatToAllBrokerWithLock() {
        if (this.lockHeartbeat.tryLock()) {
            try {
                this.sendHeartbeatToAllBroker();
                this.uploadFilterClassSource();
            } catch (final Exception e) {
                log.error("sendHeartbeatToAllBroker exception", e);
            } finally {
                this.lockHeartbeat.unlock();
            }
        } else {
            log.warn("lock heartBeat, but failed. [{}]", this.clientId);
        }
    }

    private void persistAllConsumerOffset() {
        for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            impl.persistConsumerOffset();
        }
    }

    public void adjustThreadPool() {
        for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    if (impl instanceof DefaultMQPushConsumerImpl) {
                        DefaultMQPushConsumerImpl dmq = (DefaultMQPushConsumerImpl) impl;
                        dmq.adjustThreadPool();
                    }
                } catch (Exception ignored) {
                }
            }
        }
    }

    public boolean updateTopicRouteInfoFromNameServer(final String topic) {
        return updateTopicRouteInfoFromNameServer(topic, false, null);
    }

    /**
     * 遍历 topicRouteTable 中的 BrokerData 判断该地址 是否在 topicRouteTable 存在
     * @param addr
     * @return
     */
    private boolean isBrokerAddrExistInTopicRouteTable(final String addr) {
        //遍历 topicRouteTable 中的 BrokerData 判断该地址 是否在 topicRouteTable 存在
        for (Entry<String, TopicRouteData> entry : this.topicRouteTable.entrySet()) {
            TopicRouteData topicRouteData = entry.getValue();
            List<BrokerData> bds = topicRouteData.getBrokerDatas();
            for (BrokerData bd : bds) {
                if (bd.getBrokerAddrs() != null) {
                    boolean exist = bd.getBrokerAddrs().containsValue(addr);
                    if (exist)
                        return true;
                }
            }
        }

        return false;
    }

    /**
     * 对所有Broker 实例发送其 心跳数据 包含 消费者实例 和生产者 实例  设置 broker 版本信息
     */
    private void sendHeartbeatToAllBroker() {
        //构建心跳数据
        final HeartbeatData heartbeatData = this.prepareHeartbeatData();
        final boolean producerEmpty = heartbeatData.getProducerDataSet().isEmpty();
        final boolean consumerEmpty = heartbeatData.getConsumerDataSet().isEmpty();
        //没有生产者 和 消费者信息
        if (producerEmpty && consumerEmpty) {
            log.warn("sending heartbeat, but no consumer and no producer. [{}]", this.clientId);
            return;
        }

        if (this.brokerAddrTable.isEmpty()) {
            return;
        }
        //增长 发送心跳的计数
        long times = this.sendHeartbeatTimesTotal.getAndIncrement();
        for (Entry<String, HashMap<Long, String>> brokerClusterInfo : this.brokerAddrTable.entrySet()) {
            String brokerName = brokerClusterInfo.getKey();
            HashMap<Long, String> oneTable = brokerClusterInfo.getValue();
            if (oneTable == null) {
                continue;
            }
            //遍历 broker集群 当中的 broker实例 发送心跳消息
            for (Entry<Long, String> singleBrokerInstance : oneTable.entrySet()) {
                Long id = singleBrokerInstance.getKey();
                String addr = singleBrokerInstance.getValue();
                if (addr == null) {
                    continue;
                }
                if (consumerEmpty && MixAll.MASTER_ID != id) {
                    continue;
                }

                try {
                    int version = this.mQClientAPIImpl.sendHeartbeat(addr, heartbeatData, clientConfig.getMqClientApiTimeout());
                    //初始化  broker集群  broker版本实例 map 设置 broker 地址 和 其版本号
                    if (!this.brokerVersionTable.containsKey(brokerName)) {
                        this.brokerVersionTable.put(brokerName, new HashMap<>(4));
                    }
                    this.brokerVersionTable.get(brokerName).put(addr, version);
                    if (times % 20 == 0) {
                        log.info("send heart beat to broker[{} {} {}] success", brokerName, id, addr);
                        log.info(heartbeatData.toString());
                    }
                } catch (Exception e) {
                    if (this.isBrokerInNameServer(addr)) {
                        log.info("send heart beat to broker[{} {} {}] failed", brokerName, id, addr, e);
                    } else {
                        log.info("send heart beat to broker[{} {} {}] exception, because the broker not up, forget it", brokerName,
                                id, addr, e);
                    }
                }
            }
        }
    }

    private void uploadFilterClassSource() {
        //遍历消费者实例
        for (Entry<String, MQConsumerInner> next : this.consumerTable.entrySet()) {
            MQConsumerInner consumer = next.getValue();
            if (ConsumeType.CONSUME_PASSIVELY != consumer.consumeType()) {
                continue;
            }
            //获取消费者的订阅信息
            Set<SubscriptionData> subscriptions = consumer.subscriptions();
            //如果订阅关系是类过滤,并且有有过滤类 则上传类过滤
            for (SubscriptionData sub : subscriptions) {
                if (sub.isClassFilterMode() && sub.getFilterClassSource() != null) {
                    final String consumerGroup = consumer.groupName();
                    final String className = sub.getSubString();
                    final String topic = sub.getTopic();
                    final String filterClassSource = sub.getFilterClassSource();
                    try {
                        this.uploadFilterClassToAllFilterServer(consumerGroup, className, topic, filterClassSource);
                    } catch (Exception e) {
                        log.error("uploadFilterClassToAllFilterServer Exception", e);
                    }
                }
            }
        }
    }

    public boolean updateTopicRouteInfoFromNameServer(final String topic, boolean isDefault,
        DefaultMQProducer defaultMQProducer) {
        try {
            //进行上锁
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    TopicRouteData topicRouteData;
                    //从生产者的 topic 获取 topicRouteData
                    if (isDefault && defaultMQProducer != null) {
                        //从NameServer 的 topic TBW102 获取 topicRouteData
                        topicRouteData = this.mQClientAPIImpl.getDefaultTopicRouteInfoFromNameServer(defaultMQProducer.getCreateTopicKey(),
                            clientConfig.getMqClientApiTimeout());
                        if (topicRouteData != null) {
                            //FIXME:: 设置 队列数量 ?
                            for (QueueData data : topicRouteData.getQueueDatas()) {
                                int queueNums = Math.min(defaultMQProducer.getDefaultTopicQueueNums(), data.getReadQueueNums());
                                data.setReadQueueNums(queueNums);
                                data.setWriteQueueNums(queueNums);
                            }
                        }
                    } else {
                        //从topic 当中获取  topicRouteData
                        topicRouteData = this.mQClientAPIImpl.getTopicRouteInfoFromNameServer(topic, clientConfig.getMqClientApiTimeout());
                    }
                    if (topicRouteData != null) {
                        TopicRouteData old = this.topicRouteTable.get(topic);
                        //判断TopicRouteData有没有发生改变
                        boolean changed = topicRouteData.topicRouteDataChanged(old);
                        //如果TopicRouteData没有发生改变
                        //遍历生产者 判断生产者 topic 是否需要更新
                        //遍历消费者 判断消费 订阅关系是否需要更新
                        if (!changed) {
                            changed = this.isNeedUpdateTopicRouteInfo(topic);
                        } else {
                            log.info("the topic[{}] route info changed, old[{}] ,new[{}]", topic, old, topicRouteData);
                        }
                        //TODO::更新 topic 路由信息
                        //发生改变
                        if (changed) {
                            //更新 brokerName 和  broker 映射关系
                            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                                this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
                            }

                            // Update endpoint map
                            {
                                ConcurrentMap<MessageQueue, String> mqEndPoints = topicRouteData2EndpointsForStaticTopic(topic, topicRouteData);
                                if (!mqEndPoints.isEmpty()) {
                                    topicEndPointsTable.put(topic, mqEndPoints);
                                }
                            }
                            //更显 Pub info
                            // Update Pub info
                            {
                                TopicPublishInfo publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData);
                                publishInfo.setHaveTopicRouterInfo(true);
                                for (Entry<String, MQProducerInner> entry : this.producerTable.entrySet()) {
                                    MQProducerInner impl = entry.getValue();
                                    if (impl != null) {
                                        impl.updateTopicPublishInfo(topic, publishInfo);
                                    }
                                }
                            }
                            //更新 订阅 info
                            // Update sub info
                            if (!consumerTable.isEmpty()) {
                                Set<MessageQueue> subscribeInfo = topicRouteData2TopicSubscribeInfo(topic, topicRouteData);
                                for (Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
                                    MQConsumerInner impl = entry.getValue();
                                    if (impl != null) {
                                        impl.updateTopicSubscribeInfo(topic, subscribeInfo);
                                    }
                                }
                            }
                            TopicRouteData cloneTopicRouteData = new TopicRouteData(topicRouteData);
                            log.info("topicRouteTable.put. Topic = {}, TopicRouteData[{}]", topic, cloneTopicRouteData);
                            this.topicRouteTable.put(topic, cloneTopicRouteData);
                            return true;
                        }
                    } else {
                        log.warn("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}. [{}]", topic, this.clientId);
                    }
                } catch (MQClientException e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) && !topic.equals(TopicValidator.AUTO_CREATE_TOPIC_KEY_TOPIC)) {
                        log.warn("updateTopicRouteInfoFromNameServer Exception", e);
                    }
                } catch (RemotingException e) {
                    log.error("updateTopicRouteInfoFromNameServer Exception", e);
                    throw new IllegalStateException(e);
                } finally {
                    this.lockNamesrv.unlock();
                }
            } else {
                log.warn("updateTopicRouteInfoFromNameServer tryLock timeout {}ms. [{}]", LOCK_TIMEOUT_MILLIS, this.clientId);
            }
        } catch (InterruptedException e) {
            log.warn("updateTopicRouteInfoFromNameServer Exception", e);
        }

        return false;
    }

    /**
     * 准备心跳数据 构建 该 客户端 id 相关的生产者 和 消费者 消费类型 从何处消费 订阅关系 消费模式
     * @return
     */
    private HeartbeatData prepareHeartbeatData() {
        HeartbeatData heartbeatData = new HeartbeatData();
        //客户端id
        // clientID
        heartbeatData.setClientID(this.clientId);
        //消费者信息 消费类型 从何处消费 订阅关系
        // Consumer
        for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                ConsumerData consumerData = new ConsumerData();
                consumerData.setGroupName(impl.groupName());
                consumerData.setConsumeType(impl.consumeType());
                consumerData.setMessageModel(impl.messageModel());
                consumerData.setConsumeFromWhere(impl.consumeFromWhere());
                consumerData.getSubscriptionDataSet().addAll(impl.subscriptions());
                consumerData.setUnitMode(impl.isUnitMode());

                heartbeatData.getConsumerDataSet().add(consumerData);
            }
        }
        //生产者信息
        // Producer
        for (Map.Entry<String/* group */, MQProducerInner> entry : this.producerTable.entrySet()) {
            MQProducerInner impl = entry.getValue();
            if (impl != null) {
                ProducerData producerData = new ProducerData();
                producerData.setGroupName(entry.getKey());

                heartbeatData.getProducerDataSet().add(producerData);
            }
        }

        return heartbeatData;
    }

    private boolean isBrokerInNameServer(final String brokerAddr) {
        for (Entry<String, TopicRouteData> itNext : this.topicRouteTable.entrySet()) {
            List<BrokerData> brokerDatas = itNext.getValue().getBrokerDatas();
            for (BrokerData bd : brokerDatas) {
                boolean contain = bd.getBrokerAddrs().containsValue(brokerAddr);
                if (contain)
                    return true;
            }
        }

        return false;
    }

    /**
     * 注册消息过滤的class
     * This method will be removed in the version 5.0.0,because filterServer was removed,and method
     * <code>subscribe(final String topic, final MessageSelector messageSelector)</code> is recommended.
     */
    @Deprecated
    private void uploadFilterClassToAllFilterServer(final String consumerGroup, final String fullClassName,
        final String topic,
        final String filterClassSource) {
        byte[] classBody = null;
        int classCRC = 0;
        try {
            classBody = filterClassSource.getBytes(MixAll.DEFAULT_CHARSET);
            classCRC = UtilAll.crc32(classBody);
        } catch (Exception e1) {
            log.warn("uploadFilterClassToAllFilterServer Exception, ClassName: {} {}",
                fullClassName,
                RemotingHelper.exceptionSimpleDesc(e1));
        }
        //根据根据topic 遍历 Filter Server 进行注册
        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        if (topicRouteData != null
            && topicRouteData.getFilterServerTable() != null && !topicRouteData.getFilterServerTable().isEmpty()) {
            for (Entry<String, List<String>> next : topicRouteData.getFilterServerTable().entrySet()) {
                List<String> value = next.getValue();
                for (final String fsAddr : value) {
                    try {
                        this.mQClientAPIImpl.registerMessageFilterClass(fsAddr, consumerGroup, topic, fullClassName, classCRC, classBody,
                                5000);

                        log.info("register message class filter to {} OK, ConsumerGroup: {} Topic: {} ClassName: {}", fsAddr, consumerGroup,
                                topic, fullClassName);

                    } catch (Exception e) {
                        log.error("uploadFilterClassToAllFilterServer Exception", e);
                    }
                }
            }
        } else {
            log.warn("register message class filter failed, because no filter server, ConsumerGroup: {} Topic: {} ClassName: {}",
                consumerGroup, topic, fullClassName);
        }
    }

    /**
     * 遍历生产者 判断生产者 topic 是否需要更新
     * 遍历消费者 判断消费 订阅关系是否需要更新
     * @param topic
     * @return
     */
    private boolean isNeedUpdateTopicRouteInfo(final String topic) {
        boolean result = false;
        //遍历生产者 判断生产者 topic 是否需要更新
        Iterator<Entry<String, MQProducerInner>> producerIterator = this.producerTable.entrySet().iterator();
        while (producerIterator.hasNext() && !result) {
            Entry<String, MQProducerInner> entry = producerIterator.next();
            MQProducerInner impl = entry.getValue();
            if (impl != null) {
                result = impl.isPublishTopicNeedUpdate(topic);
            }
        }

        if (result) {
            return true;
        }
        //遍历消费者 判断消费 订阅关系是否需要更新
        Iterator<Entry<String, MQConsumerInner>> consumerIterator = this.consumerTable.entrySet().iterator();
        while (consumerIterator.hasNext() && !result) {
            Entry<String, MQConsumerInner> entry = consumerIterator.next();
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                result = impl.isSubscribeTopicNeedUpdate(topic);
            }
        }

        return result;
    }

    public void shutdown() {
        // Consumer
        if (!this.consumerTable.isEmpty())
            return;

        // AdminExt
        if (!this.adminExtTable.isEmpty())
            return;

        // Producer
        if (this.producerTable.size() > 1)
            return;

        synchronized (this) {
            switch (this.serviceState) {
                case RUNNING:
                    this.defaultMQProducer.getDefaultMQProducerImpl().shutdown(false);

                    this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                    this.pullMessageService.shutdown(true);
                    this.scheduledExecutorService.shutdown();
                    this.mQClientAPIImpl.shutdown();
                    this.rebalanceService.shutdown();
                    //移除该客户端
                    MQClientManager.getInstance().removeClientFactory(this.clientId);
                    log.info("the client factory [{}] shutdown OK", this.clientId);
                    break;
                case CREATE_JUST:
                case SHUTDOWN_ALREADY:
                default:
                    break;
            }
        }
    }

    /**
     * 注册生产者 建立消费者组 和 消费者实例 的映射关系 发送心跳时候进行了 注册
     * @param group 消费者组 名
     * @param consumer 消费者实例
     * @return
     */
    public synchronized boolean registerConsumer(final String group, final MQConsumerInner consumer) {
        if (null == group || null == consumer) {
            return false;
        }

        MQConsumerInner prev = this.consumerTable.putIfAbsent(group, consumer);
        if (prev != null) {
            log.warn("the consumer group[" + group + "] exist already.");
            return false;
        }

        return true;
    }

    /**
     *  遍历 broker 集群 当中的每个节点 取消注册该消费者
     * @param group
     */
    public synchronized void unregisterConsumer(final String group) {
        this.consumerTable.remove(group);
        this.unregisterClient(null, group);
    }

    /**
     * 遍历 broker 集群 当中的每个节点 取消注册该客户端
     * @param producerGroup
     * @param consumerGroup
     */
    private void unregisterClient(final String producerGroup, final String consumerGroup) {
        //遍历 broker 集群 当中的每个节点
        for (Entry<String, HashMap<Long, String>> brokerClusterInfo : this.brokerAddrTable.entrySet()) {
            String brokerName = brokerClusterInfo.getKey();
            HashMap<Long, String> oneTable = brokerClusterInfo.getValue();

            if (oneTable == null) {
                continue;
            }
            //遍历 broker 集群 当中的每个节点 取消注册该客户端
            for (Entry<Long, String> singleBrokerInstance : oneTable.entrySet()) {
                String addr = singleBrokerInstance.getValue();
                if (addr != null) {
                    try {
                        this.mQClientAPIImpl.unregisterClient(addr, this.clientId, producerGroup, consumerGroup, clientConfig.getMqClientApiTimeout());
                        log.info("unregister client[Producer: {} Consumer: {}] from broker[{} {} {}] success", producerGroup, consumerGroup, brokerName, singleBrokerInstance.getKey(), addr);
                    } catch (RemotingException e) {
                        log.warn("unregister client RemotingException from broker: {}, {}", addr, e.getMessage());
                    } catch (InterruptedException e) {
                        log.warn("unregister client InterruptedException from broker: {}, {}", addr, e.getMessage());
                    } catch (MQBrokerException e) {
                        log.warn("unregister client MQBrokerException from broker: {}, {}", addr, e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * 注册生产者 建立生产组 和 生产者实例 的映射关系 发送心跳时候进行了 注册
     * @param group 生产者组名
     * @param producer 生产者
     * @return
     */
    public synchronized boolean registerProducer(final String group, final DefaultMQProducerImpl producer) {
        if (null == group || null == producer) {
            return false;
        }

        MQProducerInner prev = this.producerTable.putIfAbsent(group, producer);
        if (prev != null) {
            log.warn("the producer group[{}] exist already.", group);
            return false;
        }

        return true;
    }

    /**
     * 取消注册生产者
     * @param group
     */
    public synchronized void unregisterProducer(final String group) {
        this.producerTable.remove(group);
        this.unregisterClient(group, null);
    }

    public boolean registerAdminExt(final String group, final MQAdminExtInner admin) {
        if (null == group || null == admin) {
            return false;
        }

        MQAdminExtInner prev = this.adminExtTable.putIfAbsent(group, admin);
        if (prev != null) {
            log.warn("the admin group[{}] exist already.", group);
            return false;
        }

        return true;
    }

    public void unregisterAdminExt(final String group) {
        this.adminExtTable.remove(group);
    }

    /**
     * 延迟唤醒 rebalanceService
     * @param delayMillis
     */
    public void rebalanceLater(long delayMillis) {
        //delayMillis < 0 直接 进行唤醒
        //否则 定时 进行延迟 delayMillis 毫秒 唤醒
        if (delayMillis <= 0) {
            this.rebalanceService.wakeup();
        } else {
            this.scheduledExecutorService.schedule(MQClientInstance.this.rebalanceService::wakeup, delayMillis, TimeUnit.MILLISECONDS);
        }
    }

    public void rebalanceImmediately() {
        this.rebalanceService.wakeup();
    }

    public void doRebalance() {
        for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    impl.doRebalance();
                } catch (Throwable e) {
                    log.error("doRebalance exception", e);
                }
            }
        }
    }

    public MQProducerInner selectProducer(final String group) {
        return this.producerTable.get(group);
    }

    public MQConsumerInner selectConsumer(final String group) {
        return this.consumerTable.get(group);
    }

    /**
     * 从 MessageQueue 获取 BrokerName
     * @param mq
     * @return
     */
    public String getBrokerNameFromMessageQueue(final MessageQueue mq) {
        if (topicEndPointsTable.get(mq.getTopic()) != null && !topicEndPointsTable.get(mq.getTopic()).isEmpty()) {
            return topicEndPointsTable.get(mq.getTopic()).get(mq);
        }
        return mq.getBrokerName();
    }

    public FindBrokerResult findBrokerAddressInAdmin(final String brokerName) {
        if (brokerName == null) {
            return null;
        }
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            for (Map.Entry<Long, String> entry : map.entrySet()) {
                Long id = entry.getKey();
                brokerAddr = entry.getValue();
                if (brokerAddr != null) {
                    found = true;
                    slave = MixAll.MASTER_ID != id;
                    break;

                }
            } // end of for
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }

        return null;
    }

    /**
     * 根据 brokerName 获取 主 broker地址
     * @param brokerName
     * @return
     */
    public String findBrokerAddressInPublish(final String brokerName) {
        if (brokerName == null) {
            return null;
        }
        //根据 brokerName  获取 broker 再 获取主 brokerAddress
        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            return map.get(MixAll.MASTER_ID);
        }

        return null;
    }

    /**
     * 根据 broker 和 brokerId 获取 该 broker信息
     * @param brokerName
     * @param brokerId
     * @param onlyThisBroker
     * @return
     */
    public FindBrokerResult findBrokerAddressInSubscribe(
        final String brokerName,
        final long brokerId,
        final boolean onlyThisBroker
    ) {
        if (brokerName == null) {
            return null;
        }
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;
        //根据 brokerName , 获取 broker 节点列表
        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            //根据 brokerId 获取 broker地址
            brokerAddr = map.get(brokerId);
            //是否是备 broker
            slave = brokerId != MixAll.MASTER_ID;
            //found 该 brokerId 有没有被找到
            found = brokerAddr != null;
            //如果 该 broker 没有 发现 并且 该 broker 不是主 那么选取 (id + 1) 的 broker
            // 也就说若找 broker 不是主 找的是 备用 该备用 也不存在 那就找其他 备用的
            if (!found && slave) {
                brokerAddr = map.get(brokerId + 1);
                found = brokerAddr != null;
            }
            //还是没有找到 该 broker 地址  (brokerId+1 )的,并且 不是 只有该 一个 broker 那就 那就找 其中一个存在 broker
            if (!found && !onlyThisBroker) {
                Entry<Long, String> entry = map.entrySet().iterator().next();
                brokerAddr = entry.getValue();
                slave = entry.getKey() != MixAll.MASTER_ID;
                found = true;
            }
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }

        return null;
    }

    /**
     * 根据 brokerName 获取 该 broker 节点列表 然后再根据 地址 获取 该 broker 版本
     * @param brokerName
     * @param brokerAddr
     * @return
     */
    private int findBrokerVersion(String brokerName, String brokerAddr) {
        //根据brokerName 获取 该 broker 节点列表 然后再根据 地址 获取 该 broker 版本
        if (this.brokerVersionTable.containsKey(brokerName)) {
            if (this.brokerVersionTable.get(brokerName).containsKey(brokerAddr)) {
                return this.brokerVersionTable.get(brokerName).get(brokerAddr);
            }
        }
        //To do need to fresh the version
        return 0;
    }

    /**
     * 获取该 topic
     * @param topic
     * @param group
     * @return
     */
    public List<String> findConsumerIdList(final String topic, final String group) {
        //根据该 topic 找 该 topic 的 brokerAddr
        String brokerAddr = this.findBrokerAddrByTopic(topic);
        if (null == brokerAddr) {
            this.updateTopicRouteInfoFromNameServer(topic);
            brokerAddr = this.findBrokerAddrByTopic(topic);
        }

        if (null != brokerAddr) {
            try {
                // 获取该消费组的 消费者列表
                return this.mQClientAPIImpl.getConsumerIdListByGroup(brokerAddr, group, clientConfig.getMqClientApiTimeout());
            } catch (Exception e) {
                log.warn("getConsumerIdListByGroup exception, " + brokerAddr + " " + group, e);
            }
        }

        return null;
    }

    /**
     * 获取 该 topic 消费组的 消息队列的分配 策略
     * @param topic
     * @param consumerGroup
     * @param strategyName
     * @param messageModel
     * @param timeout
     * @return
     * @throws RemotingException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    public Set<MessageQueueAssignment> queryAssignment(final String topic, final String consumerGroup,
        final String strategyName, final MessageModel messageModel, int timeout)
        throws RemotingException, InterruptedException, MQBrokerException {
        //根据 topic 获取 broker 节点 列表 获取 其中一个 broker地址
        String brokerAddr = this.findBrokerAddrByTopic(topic);
        if (null == brokerAddr) {
            this.updateTopicRouteInfoFromNameServer(topic);
            brokerAddr = this.findBrokerAddrByTopic(topic);
        }
        //获取 该 topic 消费组的 消息队列的分配 策略
        if (null != brokerAddr) {
            return this.mQClientAPIImpl.queryAssignment(brokerAddr, topic, consumerGroup, clientId, strategyName,
                messageModel, timeout);
        }

        return null;
    }

    /**
     * 根据 topic 获取 broker 节点 列表 获取 其中一个 broker地址
     * @param topic
     * @return
     */
    public String findBrokerAddrByTopic(final String topic) {
        //据 topic 获取  路由信息
        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        //获取 broker 节点 列表 生获取 其中一个 broker地址
        if (topicRouteData != null) {
            List<BrokerData> brokers = topicRouteData.getBrokerDatas();
            if (!brokers.isEmpty()) {
                int index = random.nextInt(brokers.size());
                BrokerData bd = brokers.get(index % brokers.size());
                return bd.selectBrokerAddr();
            }
        }

        return null;
    }

    public synchronized void resetOffset(String topic, String group, Map<MessageQueue, Long> offsetTable) {
        DefaultMQPushConsumerImpl consumer = null;
        try {
            MQConsumerInner impl = this.consumerTable.get(group);
            if (impl instanceof DefaultMQPushConsumerImpl) {
                consumer = (DefaultMQPushConsumerImpl) impl;
            } else {
                log.info("[reset-offset] consumer dose not exist. group={}", group);
                return;
            }
            consumer.suspend();

            ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = consumer.getRebalanceImpl().getProcessQueueTable();
            for (Map.Entry<MessageQueue, ProcessQueue> entry : processQueueTable.entrySet()) {
                MessageQueue mq = entry.getKey();
                if (topic.equals(mq.getTopic()) && offsetTable.containsKey(mq)) {
                    ProcessQueue pq = entry.getValue();
                    pq.setDropped(true);
                    pq.clear();
                }
            }

            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException ignored) {
            }

            Iterator<MessageQueue> iterator = processQueueTable.keySet().iterator();
            while (iterator.hasNext()) {
                MessageQueue mq = iterator.next();
                Long offset = offsetTable.get(mq);
                if (topic.equals(mq.getTopic()) && offset != null) {
                    try {
                        consumer.updateConsumeOffset(mq, offset);
                        consumer.getRebalanceImpl().removeUnnecessaryMessageQueue(mq, processQueueTable.get(mq));
                        iterator.remove();
                    } catch (Exception e) {
                        log.warn("reset offset failed. group={}, {}", group, mq, e);
                    }
                }
            }
        } finally {
            if (consumer != null) {
                consumer.resume();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public Map<MessageQueue, Long> getConsumerStatus(String topic, String group) {
        MQConsumerInner impl = this.consumerTable.get(group);
        if (impl instanceof DefaultMQPushConsumerImpl) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else if (impl instanceof DefaultMQPullConsumerImpl) {
            DefaultMQPullConsumerImpl consumer = (DefaultMQPullConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else {
            return Collections.EMPTY_MAP;
        }
    }

    public TopicRouteData getAnExistTopicRouteData(final String topic) {
        return this.topicRouteTable.get(topic);
    }

    public MQClientAPIImpl getMQClientAPIImpl() {
        return mQClientAPIImpl;
    }

    public MQAdminImpl getMQAdminImpl() {
        return mQAdminImpl;
    }

    public long getBootTimestamp() {
        return bootTimestamp;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public PullMessageService getPullMessageService() {
        return pullMessageService;
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }

    public ConcurrentMap<String, TopicRouteData> getTopicRouteTable() {
        return topicRouteTable;
    }

    public ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg,
        final String consumerGroup,
        final String brokerName) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);
        if (mqConsumerInner instanceof DefaultMQPushConsumerImpl) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) mqConsumerInner;

            return consumer.getConsumeMessageService().consumeMessageDirectly(msg, brokerName);
        }

        return null;
    }

    public ConsumerRunningInfo consumerRunningInfo(final String consumerGroup) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);
        if (mqConsumerInner == null) {
            return null;
        }

        ConsumerRunningInfo consumerRunningInfo = mqConsumerInner.consumerRunningInfo();

        List<String> nsList = this.mQClientAPIImpl.getRemotingClient().getNameServerAddressList();

        StringBuilder strBuilder = new StringBuilder();
        if (nsList != null) {
            for (String addr : nsList) {
                strBuilder.append(addr).append(";");
            }
        }

        String nsAddr = strBuilder.toString();
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_NAMESERVER_ADDR, nsAddr);
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CONSUME_TYPE, mqConsumerInner.consumeType().name());
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CLIENT_VERSION,
            MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));

        return consumerRunningInfo;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return consumerStatsManager;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public TopicRouteData queryTopicRouteData(String topic) {
        TopicRouteData data = this.getAnExistTopicRouteData(topic);
        if (data == null) {
            this.updateTopicRouteInfoFromNameServer(topic);
            data = this.getAnExistTopicRouteData(topic);
        }
        return data;
    }
}
