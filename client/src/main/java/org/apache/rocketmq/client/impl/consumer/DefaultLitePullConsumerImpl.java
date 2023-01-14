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
package org.apache.rocketmq.client.impl.consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.TopicMessageQueueChangeListener;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class DefaultLitePullConsumerImpl implements MQConsumerInner {

    private final InternalLogger log = ClientLogger.getLog();
    /**
     * 消费时间戳
     */
    private final long consumerStartTimestamp = System.currentTimeMillis();
    /**
     * RPC钩子
     */
    private final RPCHook rpcHook;
    /**
     * 过滤消息钩子
     */
    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();
    /**
     * 服务状态
     */
    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;

    protected MQClientInstance mQClientFactory;

    private PullAPIWrapper pullAPIWrapper;
    /**
     * 偏移量存储 如果是广播模式则 采用 LocalFileOffsetStore 如果是 集群模式 则采用 RemoteBrokerOffsetStore
     */
    private OffsetStore offsetStore;

    private RebalanceImpl rebalanceImpl = new RebalanceLitePullImpl(this);

    private enum SubscriptionType {
        NONE, SUBSCRIBE, ASSIGN
    }

    private static final String NOT_RUNNING_EXCEPTION_MESSAGE = "The consumer not running, please start it first.";

    private static final String SUBSCRIPTION_CONFLICT_EXCEPTION_MESSAGE = "Subscribe and assign are mutually exclusive.";
    /**
     * the type of subscription
     * 订阅类型
     */
    private SubscriptionType subscriptionType = SubscriptionType.NONE;
    /**
     * Delay some time when exception occur
     */
    private long pullTimeDelayMillsWhenException = 1000;
    /**
     * Flow control interval 当被限流的 延迟时间
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50;
    /**
     * Delay some time when suspend pull service
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_PAUSE = 1000;

    private static final long PULL_TIME_DELAY_MILLS_ON_EXCEPTION = 3 * 1000;
    /**
     * key 为 topic, value 为订阅 表达式
     */
    private ConcurrentHashMap<String/* topic */, String/* subExpression */> topicToSubExpression = new ConcurrentHashMap<>();

    private DefaultLitePullConsumer defaultLitePullConsumer;
    /**
     * key 为 消息队列 , value 为 PullTaskImpl ; 为每个消息队列 对应的 获取消息task
     */
    private final ConcurrentMap<MessageQueue, PullTaskImpl> taskTable =
        new ConcurrentHashMap<MessageQueue, PullTaskImpl>();

    private AssignedMessageQueue assignedMessageQueue = new AssignedMessageQueue();
    /**
     * 消费请求队列
     */
    private final BlockingQueue<ConsumeRequest> consumeRequestCache = new LinkedBlockingQueue<ConsumeRequest>();
    /**
     *  调度线程 执行每个 消息队列 获取消息task
     */
    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;
    /**
     * 遍历 topicMessageQueueChangeListenerMap 如果 该 topic 的消息队列 发生
     */
    private final ScheduledExecutorService scheduledExecutorService;
    /**
     * key 为 topic , value 为 topic 对应的 TopicMessageQueueChangeListener ; topic => TopicMessageQueueChangeListener 的 映射关系
     */
    private Map<String, TopicMessageQueueChangeListener> topicMessageQueueChangeListenerMap = new HashMap<String, TopicMessageQueueChangeListener>();
    /**
     * key 为 topic , value 为 该 topic 的 MessageQueue
     */
    private Map<String, Set<MessageQueue>> messageQueuesForTopic = new HashMap<String, Set<MessageQueue>>();

    private long consumeRequestFlowControlTimes = 0L;

    private long queueFlowControlTimes = 0L;

    private long queueMaxSpanFlowControlTimes = 0L;
    /**
     * 下一次自动提交 时间
     */
    private long nextAutoCommitDeadline = -1L;
    /**
     * 消息队列的锁
     */
    private final MessageQueueLock messageQueueLock = new MessageQueueLock();
    /**
     * 消费消息钩子
     */
    private final ArrayList<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();

    // only for test purpose, will be modified by reflection in unit test.
    @SuppressWarnings("FieldMayBeFinal") private static boolean doNotUpdateTopicSubscribeInfoWhenSubscriptionChanged = false;

    public DefaultLitePullConsumerImpl(final DefaultLitePullConsumer defaultLitePullConsumer, final RPCHook rpcHook) {
        this.defaultLitePullConsumer = defaultLitePullConsumer;
        this.rpcHook = rpcHook;
        this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(
            this.defaultLitePullConsumer.getPullThreadNums(),
            new ThreadFactoryImpl("PullMsgThread-" + this.defaultLitePullConsumer.getConsumerGroup())
        );
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "MonitorMessageQueueChangeThread");
            }
        });
        this.pullTimeDelayMillsWhenException = defaultLitePullConsumer.getPullTimeDelayMillsWhenException();
    }

    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register consumeMessageHook Hook, {}", hook.hookName());
    }

    /**
     * 执行消费前置钩子
     * @param context
     */
    public void executeHookBefore(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable e) {
                    log.error("consumeMessageHook {} executeHookBefore exception", hook.hookName(), e);
                }
            }
        }
    }
    /**
     * 执行消费后置钩子
     * @param context
     */
    public void executeHookAfter(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                    log.error("consumeMessageHook {} executeHookAfter exception", hook.hookName(), e);
                }
            }
        }
    }

    private void checkServiceState() {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new IllegalStateException(NOT_RUNNING_EXCEPTION_MESSAGE);
        }
    }

    /**
     * 更新 NameServerAddr
     * @param newAddresses
     */
    public void updateNameServerAddr(String newAddresses) {
        this.mQClientFactory.getMQClientAPIImpl().updateNameServerAddressList(newAddresses);
    }

    /**
     * 设置订阅类型只有 订阅类型为NONE 时候 才难呢过设置
     * @param type
     */
    private synchronized void setSubscriptionType(SubscriptionType type) {
        if (this.subscriptionType == SubscriptionType.NONE) {
            this.subscriptionType = type;
        } else if (this.subscriptionType != type) {
            throw new IllegalStateException(SUBSCRIPTION_CONFLICT_EXCEPTION_MESSAGE);
        }
    }

    /**
     * 遍历 assignedMessageQueueState 将 topic 下 不在 assigned 标记丢弃 进行移除
     * 将 assigned 中 消息队列 添加 到 assignedMessageQueueState
     * @param topic
     * @param assignedMessageQueue
     */
    private void updateAssignedMessageQueue(String topic, Set<MessageQueue> assignedMessageQueue) {
        this.assignedMessageQueue.updateAssignedMessageQueue(topic, assignedMessageQueue);
    }

    private void updatePullTask(String topic, Set<MessageQueue> mqNewSet) {
        //遍历 taskTable 将 该 topic 的 不在 该 mqNewSet 进行移除 然后
        Iterator<Map.Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, PullTaskImpl> next = it.next();
            if (next.getKey().getTopic().equals(topic)) {
                if (!mqNewSet.contains(next.getKey())) {
                    next.getValue().setCancelled(true);
                    it.remove();
                }
            }
        }
        //遍历 mqSet 如果不 存在 该消息 对应 获取消息 请求 则生成获取 消息请求 然后提交获取消息请求
        startPullTask(mqNewSet);
    }

    /**
     * 消息队列发生改变需要
     */
    class MessageQueueListenerImpl implements MessageQueueListener {
        @Override
        public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
            updateAssignQueueAndStartPullTask(topic, mqAll, mqDivided);
        }
    }

    /**
     * 如果是广播模式 则 更新 mqAll 广播模式下所有 消息队列一个 消息队列状态
     * 如果是集群模式 则 更新 mqDivided
     * @param topic
     * @param mqAll
     * @param mqDivided
     */
    public void updateAssignQueueAndStartPullTask(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
        MessageModel messageModel = defaultLitePullConsumer.getMessageModel();
        switch (messageModel) {
            case BROADCASTING:
                //如果是广播模式 则 更新 mqAll 广播模式下所有 消息队列一个 消息队列状态
                updateAssignedMessageQueue(topic, mqAll);
                updatePullTask(topic, mqAll);
                break;
            case CLUSTERING:
                //如果是集群模式 则 更新 mqDivided
                updateAssignedMessageQueue(topic, mqDivided);
                updatePullTask(topic, mqDivided);
                break;
            default:
                break;
        }
    }

    public synchronized void shutdown() {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                //持久化消息队列消费偏移量
                persistConsumerOffset();
                //移除该消费者
                this.mQClientFactory.unregisterConsumer(this.defaultLitePullConsumer.getConsumerGroup());
                //关闭定时任务
                scheduledThreadPoolExecutor.shutdown();
                scheduledExecutorService.shutdown();
                this.mQClientFactory.shutdown();
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                log.info("the consumer [{}] shutdown OK", this.defaultLitePullConsumer.getConsumerGroup());
                break;
            default:
                break;
        }
    }

    public synchronized boolean isRunning() {
        return this.serviceState == ServiceState.RUNNING;
    }

    public synchronized void start() throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                this.serviceState = ServiceState.START_FAILED;
                //检查配置
                this.checkConfig();
                //如果是集群 实例名称是DEFAULT 则将其改成 Pid#System.nanoTim
                if (this.defaultLitePullConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                    this.defaultLitePullConsumer.changeInstanceNameToPID();
                }

                initMQClientFactory();

                initRebalanceImpl();

                initPullAPIWrapper();

                initOffsetStore();

                mQClientFactory.start();

                startScheduleTask();

                this.serviceState = ServiceState.RUNNING;

                log.info("the consumer [{}] start OK", this.defaultLitePullConsumer.getConsumerGroup());

                operateAfterRunning();

                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The PullConsumer service state not OK, maybe started once, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
            default:
                break;
        }
    }

    private void initMQClientFactory() throws MQClientException {
        //初始化该客户端 注册该消费者
        this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultLitePullConsumer, this.rpcHook);
        boolean registerOK = mQClientFactory.registerConsumer(this.defaultLitePullConsumer.getConsumerGroup(), this);
        if (!registerOK) {
            this.serviceState = ServiceState.CREATE_JUST;

            throw new MQClientException("The consumer group[" + this.defaultLitePullConsumer.getConsumerGroup()
                + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                null);
        }
    }

    /**
     * 初始化 RebalanceImpl
     */
    private void initRebalanceImpl() {
        this.rebalanceImpl.setConsumerGroup(this.defaultLitePullConsumer.getConsumerGroup());
        this.rebalanceImpl.setMessageModel(this.defaultLitePullConsumer.getMessageModel());
        this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultLitePullConsumer.getAllocateMessageQueueStrategy());
        this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);
    }

    /**
     * 初始化 pullAPIWrapper
     */
    private void initPullAPIWrapper() {
        this.pullAPIWrapper = new PullAPIWrapper(
            mQClientFactory,
            this.defaultLitePullConsumer.getConsumerGroup(), isUnitMode());
        this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);
    }

    /**
     * 初始化偏移量 如果是广播模式则 采用 LocalFileOffsetStore 如果是 集群模式 则采用 RemoteBrokerOffsetStore
     * @throws MQClientException
     */
    private void initOffsetStore() throws MQClientException {
        if (this.defaultLitePullConsumer.getOffsetStore() != null) {
            this.offsetStore = this.defaultLitePullConsumer.getOffsetStore();
        } else {
            switch (this.defaultLitePullConsumer.getMessageModel()) {
                case BROADCASTING:
                    this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultLitePullConsumer.getConsumerGroup());
                    break;
                case CLUSTERING:
                    this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultLitePullConsumer.getConsumerGroup());
                    break;
                default:
                    break;
            }
            this.defaultLitePullConsumer.setOffsetStore(this.offsetStore);
        }
        this.offsetStore.load();
    }

    private void startScheduleTask() {
        scheduledExecutorService.scheduleAtFixedRate(
            new Runnable() {
                @Override
                public void run() {
                    try {
                        //遍历 topicMessageQueueChangeListenerMap 如果 该 topic 的消息队列 发生
                        fetchTopicMessageQueuesAndCompare();
                    } catch (Exception e) {
                        log.error("ScheduledTask fetchMessageQueuesAndCompare exception", e);
                    }
                }
            }, 1000 * 10, this.getDefaultLitePullConsumer().getTopicMetadataCheckIntervalMillis(), TimeUnit.MILLISECONDS);
    }

    private void operateAfterRunning() throws MQClientException {
        // If subscribe function invoke before start function, then update topic subscribe info after initialization.
        //如果订阅类型 则  遍历订阅信息 更新 对应 topic 的 路由信息
        if (subscriptionType == SubscriptionType.SUBSCRIBE) {
            updateTopicSubscribeInfoWhenSubscriptionChanged();
        }
        // If assign function invoke before start function, then update pull task after initialization.
        //如果是ASSIGN 则根据 遍历 mqSet 如果不 存在 该消息 对应 获取消息 请求 则生成获取 消息请求 然后提交获取消息请求
        if (subscriptionType == SubscriptionType.ASSIGN) {
            updateAssignPullTask(assignedMessageQueue.messageQueues());
        }
        //遍历 topicMessageQueueChangeListenerMap 获取 topic 的 消息 队列 进行映射 messageQueuesForTopic
        for (String topic : topicMessageQueueChangeListenerMap.keySet()) {
            Set<MessageQueue> messageQueues = fetchMessageQueues(topic);
            messageQueuesForTopic.put(topic, messageQueues);
        }
        this.mQClientFactory.checkClientInBroker();
    }

    private void checkConfig() throws MQClientException {
        // Check consumerGroup
        Validators.checkGroup(this.defaultLitePullConsumer.getConsumerGroup());

        // Check consumerGroup name is not equal default consumer group name.
        if (this.defaultLitePullConsumer.getConsumerGroup().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
            throw new MQClientException(
                "consumerGroup can not equal "
                    + MixAll.DEFAULT_CONSUMER_GROUP
                    + ", please specify another one."
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // Check messageModel is not null.
        if (null == this.defaultLitePullConsumer.getMessageModel()) {
            throw new MQClientException(
                "messageModel is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // Check allocateMessageQueueStrategy is not null
        if (null == this.defaultLitePullConsumer.getAllocateMessageQueueStrategy()) {
            throw new MQClientException(
                "allocateMessageQueueStrategy is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        if (this.defaultLitePullConsumer.getConsumerTimeoutMillisWhenSuspend() < this.defaultLitePullConsumer.getBrokerSuspendMaxTimeMillis()) {
            throw new MQClientException(
                "Long polling mode, the consumer consumerTimeoutMillisWhenSuspend must greater than brokerSuspendMaxTimeMillis"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
    }

    public PullAPIWrapper getPullAPIWrapper() {
        return pullAPIWrapper;
    }

    /**
     * 遍历 mqSet 如果不 存在 该消息 对应 获取消息 请求 则生成获取 消息请求 然后提交获取消息请求
     * @param mqSet
     */
    private void startPullTask(Collection<MessageQueue> mqSet) {
        //遍历 mqSet 如果不 存在 该消息 对应 获取消息 请求 则生成获取 消息请求 然后提交获取消息请求
        for (MessageQueue messageQueue : mqSet) {
            if (!this.taskTable.containsKey(messageQueue)) {
                PullTaskImpl pullTask = new PullTaskImpl(messageQueue);
                this.taskTable.put(messageQueue, pullTask);
                this.scheduledThreadPoolExecutor.schedule(pullTask, 0, TimeUnit.MILLISECONDS);
            }
        }
    }

    private void updateAssignPullTask(Collection<MessageQueue> mqNewSet) {
        //遍历 taskTable 将 不在 该 mqNewSet 进行移除 然后
        Iterator<Map.Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, PullTaskImpl> next = it.next();
            if (!mqNewSet.contains(next.getKey())) {
                next.getValue().setCancelled(true);
                it.remove();
            }
        }
        //遍历 mqSet 如果不 存在 该消息 对应 获取消息 请求 则生成获取 消息请求 然后提交获取消息请求
        startPullTask(mqNewSet);
    }

    /**
     * 遍历订阅信息 更新 对应 topic 的 路由信息
     */
    private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
        if (doNotUpdateTopicSubscribeInfoWhenSubscriptionChanged) {
            return;
        }
        //遍历订阅信息 更新 对应 topic 的 路由信息
        Map<String, SubscriptionData> subTable = rebalanceImpl.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            }
        }
    }

    /**
     * subscribe data by customizing messageQueueListener
     * @param topic
     * @param subExpression
     * @param messageQueueListener
     * @throws MQClientException
     */
    public synchronized void subscribe(String topic, String subExpression, MessageQueueListener messageQueueListener) throws MQClientException {
        try {
            if (StringUtils.isEmpty(topic)) {
                throw new IllegalArgumentException("Topic can not be null or empty.");
            }
            //设置订阅类型 构建订阅信息 进行 topic 订阅信息的映射
            setSubscriptionType(SubscriptionType.SUBSCRIBE);
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subExpression);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            //设置监听者 消息队列发生  如果是广播模式 则 更新 mqAll 广播模式下所有 消息队列一个 消息队列状态 如果是集群模式 则 更新 mqDivided
            // 然后 在 调用 messageQueueChanged
            this.defaultLitePullConsumer.setMessageQueueListener(new MessageQueueListener() {
                @Override
                public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
                    // First, update the assign queue
                    updateAssignQueueAndStartPullTask(topic, mqAll, mqDivided);
                    // run custom listener
                    messageQueueListener.messageQueueChanged(topic, mqAll, mqDivided);
                }
            });
            //为 AssignedMessageQueue 设置 rebalanceImpl
            assignedMessageQueue.setRebalanceImpl(this.rebalanceImpl);
            //发送心跳 遍历订阅信息 更新 对应 topic 的 路由信息
            if (serviceState == ServiceState.RUNNING) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
                updateTopicSubscribeInfoWhenSubscriptionChanged();
            }
        } catch (Exception e) {
            throw new MQClientException("subscribe exception", e);
        }
    }


    public synchronized void subscribe(String topic, String subExpression) throws MQClientException {
        try {
            if (topic == null || "".equals(topic)) {
                throw new IllegalArgumentException("Topic can not be null or empty.");
            }
            setSubscriptionType(SubscriptionType.SUBSCRIBE);
            //设置订阅类型 构建订阅信息 进行 topic 订阅信息的映射
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subExpression);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            //设置监听者 消息队列发生  如果是广播模式 则 更新 mqAll 广播模式下所有 消息队列一个 消息队列状态 如果是集群模式 则 更新 mqDivided
            this.defaultLitePullConsumer.setMessageQueueListener(new MessageQueueListenerImpl());
            //为 AssignedMessageQueue 设置 rebalanceImpl
            assignedMessageQueue.setRebalanceImpl(this.rebalanceImpl);
            //发送心跳 遍历订阅信息 更新 对应 topic 的 路由信息
            if (serviceState == ServiceState.RUNNING) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
                updateTopicSubscribeInfoWhenSubscriptionChanged();
            }
        } catch (Exception e) {
            throw new MQClientException("subscribe exception", e);
        }
    }

    public synchronized void subscribe(String topic, MessageSelector messageSelector) throws MQClientException {
        try {
            if (topic == null || "".equals(topic)) {
                throw new IllegalArgumentException("Topic can not be null or empty.");
            }
            setSubscriptionType(SubscriptionType.SUBSCRIBE);
            if (messageSelector == null) {
                subscribe(topic, SubscriptionData.SUB_ALL);
                return;
            }
            //设置订阅类型 构建订阅信息 进行 topic 订阅信息的映射
            SubscriptionData subscriptionData = FilterAPI.build(topic,
                messageSelector.getExpression(), messageSelector.getExpressionType());
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            this.defaultLitePullConsumer.setMessageQueueListener(new MessageQueueListenerImpl());
            assignedMessageQueue.setRebalanceImpl(this.rebalanceImpl);
            if (serviceState == ServiceState.RUNNING) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
                updateTopicSubscribeInfoWhenSubscriptionChanged();
            }
        } catch (Exception e) {
            throw new MQClientException("subscribe exception", e);
        }
    }

    public synchronized void unsubscribe(final String topic) {
        //移除该 topic 的 订阅信息
        this.rebalanceImpl.getSubscriptionInner().remove(topic);
        //遍历 taskTable 移除 该 topic 的消息 队列 取消 该消息 队列 PullTaskImpl
        removePullTaskCallback(topic);
        //移除该topic 的 消息队列
        assignedMessageQueue.removeAssignedMessageQueue(topic);
    }

    public synchronized void assign(Collection<MessageQueue> messageQueues) {
        if (messageQueues == null || messageQueues.isEmpty()) {
            throw new IllegalArgumentException("Message queues can not be null or empty.");
        }
        setSubscriptionType(SubscriptionType.ASSIGN);
        // 遍历 assignedMessageQueueState  不在 assigned 标记丢弃 进行移除 将 assigned 中 消息队列 添加 到 assignedMessageQueueState
        assignedMessageQueue.updateAssignedMessageQueue(messageQueues);
        //添加对应消息队列 PullTaskImpl 任务
        if (serviceState == ServiceState.RUNNING) {
            updateAssignPullTask(messageQueues);
        }
    }

    /**
     * 设置 topic 的 订阅表达式
     * @param topic
     * @param subExpression
     */
    public synchronized void setSubExpressionForAssign(final String topic, final String subExpression) {
        if (StringUtils.isBlank(subExpression)) {
            throw new IllegalArgumentException("subExpression can not be null or empty.");
        }
        if (serviceState != ServiceState.CREATE_JUST) {
            throw new IllegalStateException("setAssignTag only can be called before start.");
        }
        setSubscriptionType(SubscriptionType.ASSIGN);
        topicToSubExpression.put(topic, subExpression);
    }

    /**
     * 超过自动提交时间 则进行提交 更新偏移量
     */
    private void maybeAutoCommit() {
        long now = System.currentTimeMillis();
        //超过 自动 提交时间
        if (now >= nextAutoCommitDeadline) {
            commitAll();
            //更新下一次自动提交 时间
            nextAutoCommitDeadline = now + defaultLitePullConsumer.getAutoCommitIntervalMillis();
        }
    }

    /**
     * 获取消费 请求 返回对应 消息
     * @param timeout
     * @return
     */
    public synchronized List<MessageExt> poll(long timeout) {
        try {
            checkServiceState();
            if (timeout < 0) {
                throw new IllegalArgumentException("Timeout must not be negative");
            }
            //自动提交 则根据 该 自动提交 时间 更新偏移量
            if (defaultLitePullConsumer.isAutoCommit()) {
                maybeAutoCommit();
            }
            long endTime = System.currentTimeMillis() + timeout;
            //获取消费请求
            ConsumeRequest consumeRequest = consumeRequestCache.poll(endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
            //如果 已经超时 则 继续获取
            if (endTime - System.currentTimeMillis() > 0) {
                while (consumeRequest != null && consumeRequest.getProcessQueue().isDropped()) {
                    consumeRequest = consumeRequestCache.poll(endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                    if (endTime - System.currentTimeMillis() <= 0) {
                        break;
                    }
                }
            }

            if (consumeRequest != null && !consumeRequest.getProcessQueue().isDropped()) {
                //获取 消息集合 从 处理队列 当中进行 移除 计算偏移量 然后更新该消费队列的 消费 偏移量
                List<MessageExt> messages = consumeRequest.getMessageExts();
                long offset = consumeRequest.getProcessQueue().removeMessage(messages);
                assignedMessageQueue.updateConsumeOffset(consumeRequest.getMessageQueue(), offset);
                //If namespace not null , reset Topic without namespace.
                this.resetTopic(messages);
                if (!this.consumeMessageHookList.isEmpty()) {
                    ConsumeMessageContext consumeMessageContext = new ConsumeMessageContext();
                    consumeMessageContext.setNamespace(defaultLitePullConsumer.getNamespace());
                    consumeMessageContext.setConsumerGroup(this.groupName());
                    consumeMessageContext.setMq(consumeRequest.getMessageQueue());
                    consumeMessageContext.setMsgList(messages);
                    consumeMessageContext.setSuccess(false);
                    //执行消费前置钩子 和 执行消费 后置钩子 返回消息
                    this.executeHookBefore(consumeMessageContext);
                    consumeMessageContext.setStatus(ConsumeConcurrentlyStatus.CONSUME_SUCCESS.toString());
                    consumeMessageContext.setSuccess(true);
                    this.executeHookAfter(consumeMessageContext);
                }
                return messages;
            }
        } catch (InterruptedException ignore) {

        }

        return Collections.emptyList();
    }

    /**
     * 暂停该消息队列
     * @param messageQueues
     */
    public void pause(Collection<MessageQueue> messageQueues) {
        assignedMessageQueue.pause(messageQueues);
    }

    /**
     * 恢复该消息队列
     * @param messageQueues
     */
    public void resume(Collection<MessageQueue> messageQueues) {
        assignedMessageQueue.resume(messageQueues);
    }

    public synchronized void seek(MessageQueue messageQueue, long offset) throws MQClientException {
        if (!assignedMessageQueue.messageQueues().contains(messageQueue)) {
            if (subscriptionType == SubscriptionType.SUBSCRIBE) {
                throw new MQClientException("The message queue is not in assigned list, may be rebalancing, message queue: " + messageQueue, null);
            } else {
                throw new MQClientException("The message queue is not in assigned list, message queue: " + messageQueue, null);
            }
        }
        //获取该消息队列的最小 最大偏移量量
        long minOffset = minOffset(messageQueue);
        long maxOffset = maxOffset(messageQueue);
        if (offset < minOffset || offset > maxOffset) {
            throw new MQClientException("Seek offset illegal, seek offset = " + offset + ", min offset = " + minOffset + ", max offset = " + maxOffset, null);
        }
        //获取该消息队列的锁
        final Object objLock = messageQueueLock.fetchLockObject(messageQueue);
        synchronized (objLock) {
            // 清该消息队列 信息 包括其对应的消费请求
            clearMessageQueueInCache(messageQueue);
            //获取该消息队列的 旧的PullTaskImpl 进行中断 移除
            PullTaskImpl oldPullTaskImpl = this.taskTable.get(messageQueue);
            if (oldPullTaskImpl != null) {
                oldPullTaskImpl.tryInterrupt();
                this.taskTable.remove(messageQueue);
            }
            //设置该消息队列的 seekOffset
            //然后创建该消息队列对应的 PullTaskImpl
            assignedMessageQueue.setSeekOffset(messageQueue, offset);
            if (!this.taskTable.containsKey(messageQueue)) {
                PullTaskImpl pullTask = new PullTaskImpl(messageQueue);
                this.taskTable.put(messageQueue, pullTask);
                this.scheduledThreadPoolExecutor.schedule(pullTask, 0, TimeUnit.MILLISECONDS);
            }
        }
    }

    public void seekToBegin(MessageQueue messageQueue) throws MQClientException {
        long begin = minOffset(messageQueue);
        this.seek(messageQueue, begin);
    }

    public void seekToEnd(MessageQueue messageQueue) throws MQClientException {
        long end = maxOffset(messageQueue);
        this.seek(messageQueue, end);
    }

    private long maxOffset(MessageQueue messageQueue) throws MQClientException {
        checkServiceState();
        return this.mQClientFactory.getMQAdminImpl().maxOffset(messageQueue);
    }

    private long minOffset(MessageQueue messageQueue) throws MQClientException {
        checkServiceState();
        return this.mQClientFactory.getMQAdminImpl().minOffset(messageQueue);
    }

    /**
     * 遍历 taskTable 移除 该 topic 的消息 队列 取消 该消息 队列 PullTaskImpl
     * @param topic
     */
    private void removePullTaskCallback(final String topic) {
        removePullTask(topic);
    }

    /**
     * 遍历 taskTable 移除 该 topic 的消息 队列 取消 该消息 队列 PullTaskImpl
     * @param topic
     */
    private void removePullTask(final String topic) {
        //遍历 taskTable 移除 该 topic 的消息 队列 取消 该消息 队列 PullTaskImpl
        Iterator<Map.Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, PullTaskImpl> next = it.next();
            if (next.getKey().getTopic().equals(topic)) {
                next.getValue().setCancelled(true);
                it.remove();
            }
        }
    }

    public synchronized void commitAll() {
        //遍历消息 消息队列 进行 提交
        for (MessageQueue messageQueue : assignedMessageQueue.messageQueues()) {
            try {
                commit(messageQueue);
            } catch (Exception e) {
                log.error("An error occurred when update consume offset Automatically.");
            }
        }
    }

    /**
     * 遍历该消费队列对应的偏移量 更新消费偏移量 然后进行持久化
     * Specify offset commit
     * @param messageQueues
     * @param persist
     */
    public synchronized void commit(final Map<MessageQueue, Long> messageQueues, boolean persist) {
        if (messageQueues == null || messageQueues.size() == 0) {
            log.warn("MessageQueues is empty, Ignore this commit ");
            return;
        }
        //遍历该消费队列对应的偏移量 更新消费偏移量 然后进行持久化
        for (Map.Entry<MessageQueue, Long> messageQueueEntry : messageQueues.entrySet()) {
            MessageQueue messageQueue = messageQueueEntry.getKey();
            long offset = messageQueueEntry.getValue();
            if (offset != -1) {
                ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);
                if (processQueue != null && !processQueue.isDropped()) {
                    updateConsumeOffset(messageQueue, offset);
                }
            } else {
                log.error("consumerOffset is -1 in messageQueue [" + messageQueue + "].");
            }
        }
        //进行持久化
        if (persist) {
            this.offsetStore.persistAll(messageQueues.keySet());
        }
    }

    /**
     * Get the queue assigned in subscribe mode
     * @return
     */
    public synchronized Set<MessageQueue> assignment() {
        return assignedMessageQueue.getAssignedMessageQueues();
    }

    public synchronized void commit(final Set<MessageQueue> messageQueues, boolean persist) {
        if (messageQueues == null || messageQueues.size() == 0) {
            return;
        }

        for (MessageQueue messageQueue : messageQueues) {
            commit(messageQueue);
        }

        if (persist) {
            this.offsetStore.persistAll(messageQueues);
        }
    }

    /**
     * 获取消息队列的的偏移量 更新消息队列偏移量
     * @param messageQueue
     */
    private synchronized void commit(MessageQueue messageQueue) {
        //获取消息队列的的偏移量 更新消息队列偏移量
        long consumerOffset = assignedMessageQueue.getConsumerOffset(messageQueue);
        if (consumerOffset != -1) {
            ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);
            if (processQueue != null && !processQueue.isDropped()) {
                updateConsumeOffset(messageQueue, consumerOffset);
            }
        } else {
            log.error("consumerOffset is -1 in messageQueue [" + messageQueue + "].");
        }
    }

    /**
     * 如果该 消息队列 seekOffset 是 -1 就  更新消息队列获取消息偏移量
     * @param messageQueue
     * @param nextPullOffset
     * @param processQueue
     */
    private void updatePullOffset(MessageQueue messageQueue, long nextPullOffset, ProcessQueue processQueue) {
        if (assignedMessageQueue.getSeekOffset(messageQueue) == -1) {
            assignedMessageQueue.updatePullOffset(messageQueue, nextPullOffset, processQueue);
        }
    }

    /**
     * 提交消费请求
     * @param consumeRequest
     */
    private void submitConsumeRequest(ConsumeRequest consumeRequest) {
        try {
            consumeRequestCache.put(consumeRequest);
        } catch (InterruptedException e) {
            log.error("Submit consumeRequest error", e);
        }
    }

    /**
     * 根据 ConsumeFromWhere 获取消费偏移量
     * @param messageQueue
     * @return
     * @throws MQClientException
     */
    private long fetchConsumeOffset(MessageQueue messageQueue) throws MQClientException {
        checkServiceState();
        long offset = this.rebalanceImpl.computePullFromWhereWithException(messageQueue);
        return offset;
    }

    /**
     * 先从内存读取然后再从磁盘读取 读取该消息队列的偏移量
     * @param messageQueue
     * @return
     * @throws MQClientException
     */
    public long committed(MessageQueue messageQueue) throws MQClientException {
        checkServiceState();
        long offset = this.offsetStore.readOffset(messageQueue, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
        if (offset == -2) {
            throw new MQClientException("Fetch consume offset from broker exception", null);
        }
        return offset;
    }

    /**
     * 清该消息队列 信息 包括其对应的消费请求
     * @param messageQueue
     */
    private void clearMessageQueueInCache(MessageQueue messageQueue) {
        ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);
        if (processQueue != null) {
            processQueue.clear();
        }
        Iterator<ConsumeRequest> iter = consumeRequestCache.iterator();
        while (iter.hasNext()) {
            if (iter.next().getMessageQueue().equals(messageQueue)) {
                iter.remove();
            }
        }
    }

    /**
     *获取该消息队列的 seekOffset 将 该消息队列的消费偏移量 更新至 seekOffset 然后将该消息队列的 seekOffset 设置成 -1
     *如果不存在  seekOffset 则直接获取该消息队列的 PullOffset 如果不存在 PullOffset 则获取该下消息队列的 消费偏移量
     * @param messageQueue
     * @return
     * @throws MQClientException
     */
    private long nextPullOffset(MessageQueue messageQueue) throws MQClientException {
        long offset = -1;
        //获取该消息队列的 seekOffset 将 该消息队列的消费偏移量 更新至 seekOffset 然后将该消息队列的 seekOffset 设置成 -1
        //如果不存在  seekOffset 则直接获取该消息队列的 PullOffset 如果不存在 PullOffset 则获取该下消息队列的 消费偏移量
        long seekOffset = assignedMessageQueue.getSeekOffset(messageQueue);
        if (seekOffset != -1) {
            offset = seekOffset;
            assignedMessageQueue.updateConsumeOffset(messageQueue, offset);
            assignedMessageQueue.setSeekOffset(messageQueue, -1);
        } else {
            offset = assignedMessageQueue.getPullOffset(messageQueue);
            if (offset == -1) {
                offset = fetchConsumeOffset(messageQueue);
            }
        }
        return offset;
    }

    /**
     *  查询 指定时间 消费队列的偏移量
     * @param mq
     * @param timestamp
     * @return
     * @throws MQClientException
     */
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        checkServiceState();
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    public class PullTaskImpl implements Runnable {
        private final MessageQueue messageQueue;
        /**
         * 是否取消
         */
        private volatile boolean cancelled = false;
        /**
         * 当前线程对象
         */
        private Thread currentThread;

        public PullTaskImpl(final MessageQueue messageQueue) {
            this.messageQueue = messageQueue;
        }

        /**
         * 将设置取消 然后 中断
         */
        public void tryInterrupt() {
            setCancelled(true);
            if (currentThread == null) {
                return;
            }
            if (!currentThread.isInterrupted()) {
                currentThread.interrupt();
            }
        }

        @Override
        public void run() {
            //获取该消息队列 的消息 更新 该消息队列的偏移量
            if (!this.isCancelled()) {

                this.currentThread = Thread.currentThread();
                //判断该消息队列是否中断
                if (assignedMessageQueue.isPaused(messageQueue)) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_PAUSE, TimeUnit.MILLISECONDS);
                    log.debug("Message Queue: {} has been paused!", messageQueue);
                    return;
                }
                //判断该处理队列是否丢弃
                ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);

                if (null == processQueue || processQueue.isDropped()) {
                    log.info("The message queue not be able to poll, because it's dropped. group={}, messageQueue={}", defaultLitePullConsumer.getConsumerGroup(), this.messageQueue);
                    return;
                }
                //超过消费阈值
                if ((long) consumeRequestCache.size() * defaultLitePullConsumer.getPullBatchSize() > defaultLitePullConsumer.getPullThresholdForAll()) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if ((consumeRequestFlowControlTimes++ % 1000) == 0) {
                        log.warn("The consume request count exceeds threshold {}, so do flow control, consume request count={}, flowControlTimes={}", consumeRequestCache.size(), consumeRequestFlowControlTimes);
                    }
                    return;
                }
                //处理队列中消息数量大小 和 消息的 大小
                long cachedMessageCount = processQueue.getMsgCount().get();
                long cachedMessageSizeInMiB = processQueue.getMsgSize().get() / (1024 * 1024);
                //数量超过阈值 延迟
                if (cachedMessageCount > defaultLitePullConsumer.getPullThresholdForQueue()) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if ((queueFlowControlTimes++ % 1000) == 0) {
                        log.warn(
                            "The cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, flowControlTimes={}",
                            defaultLitePullConsumer.getPullThresholdForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, queueFlowControlTimes);
                    }
                    return;
                }
                //大小超过阈值 延迟
                if (cachedMessageSizeInMiB > defaultLitePullConsumer.getPullThresholdSizeForQueue()) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if ((queueFlowControlTimes++ % 1000) == 0) {
                        log.warn(
                            "The cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, flowControlTimes={}",
                            defaultLitePullConsumer.getPullThresholdSizeForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, queueFlowControlTimes);
                    }
                    return;
                }
                //超过最大跨度进行限流
                if (processQueue.getMaxSpan() > defaultLitePullConsumer.getConsumeMaxSpan()) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if ((queueMaxSpanFlowControlTimes++ % 1000) == 0) {
                        log.warn(
                            "The queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, flowControlTimes={}",
                            processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), processQueue.getMaxSpan(), queueMaxSpanFlowControlTimes);
                    }
                    return;
                }

                long offset = 0L;
                try {
                    //获取偏移量
                    offset = nextPullOffset(messageQueue);
                } catch (Exception e) {
                    log.error("Failed to get next pull offset", e);
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_ON_EXCEPTION, TimeUnit.MILLISECONDS);
                    return;
                }

                if (this.isCancelled() || processQueue.isDropped()) {
                    return;
                }
                long pullDelayTimeMills = 0;
                try {
                    SubscriptionData subscriptionData;
                    String topic = this.messageQueue.getTopic();
                    //如果是该消费者是订阅类型 则获取 改下消费者 订阅信息
                    if (subscriptionType == SubscriptionType.SUBSCRIBE) {
                        subscriptionData = rebalanceImpl.getSubscriptionInner().get(topic);
                    } else {
                        //获取该 topic 的订阅 表达 构建订阅信息
                        String subExpression4Assign = topicToSubExpression.get(topic);
                        subExpression4Assign = subExpression4Assign == null ? SubscriptionData.SUB_ALL : subExpression4Assign;
                        subscriptionData = FilterAPI.buildSubscriptionData(topic, subExpression4Assign);
                    }

                    PullResult pullResult = pull(messageQueue, subscriptionData, offset, defaultLitePullConsumer.getPullBatchSize());
                    if (this.isCancelled() || processQueue.isDropped()) {
                        return;
                    }
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            //获取该消息队列的锁
                            final Object objLock = messageQueueLock.fetchLockObject(messageQueue);
                            synchronized (objLock) {
                                //将获取的到的消息添加 processQueue 待消费消息中 然后提交消费请求
                                if (pullResult.getMsgFoundList() != null && !pullResult.getMsgFoundList().isEmpty() && assignedMessageQueue.getSeekOffset(messageQueue) == -1) {
                                    processQueue.putMessage(pullResult.getMsgFoundList());
                                    submitConsumeRequest(new ConsumeRequest(pullResult.getMsgFoundList(), messageQueue, processQueue));
                                }
                            }
                            break;
                        case OFFSET_ILLEGAL:
                            log.warn("The pull request offset illegal, {}", pullResult.toString());
                            break;
                        default:
                            break;
                    }
                    //如果该 消息队列 seekOffset 是 -1 就  更新消息队列获取消息偏移量
                    updatePullOffset(messageQueue, pullResult.getNextBeginOffset(), processQueue);
                } catch (InterruptedException interruptedException) {
                    log.warn("Polling thread was interrupted.", interruptedException);
                } catch (Throwable e) {
                    pullDelayTimeMills = pullTimeDelayMillsWhenException;
                    log.error("An error occurred in pull message process.", e);
                }
                //如果没有取消则 继续提交获取消息
                if (!this.isCancelled()) {
                    scheduledThreadPoolExecutor.schedule(this, pullDelayTimeMills, TimeUnit.MILLISECONDS);
                } else {
                    log.warn("The Pull Task is cancelled after doPullTask, {}", messageQueue);
                }
            }
        }

        public boolean isCancelled() {
            return cancelled;
        }

        public void setCancelled(boolean cancelled) {
            this.cancelled = cancelled;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }
    }

    private PullResult pull(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return pull(mq, subscriptionData, offset, maxNums, this.defaultLitePullConsumer.getConsumerPullTimeoutMillis());
    }

    private PullResult pull(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.pullSyncImpl(mq, subscriptionData, offset, maxNums, true, timeout);
    }

    private PullResult pullSyncImpl(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums,
        boolean block,
        long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        if (null == mq) {
            throw new MQClientException("mq is null", null);
        }

        if (offset < 0) {
            throw new MQClientException("offset < 0", null);
        }

        if (maxNums <= 0) {
            throw new MQClientException("maxNums <= 0", null);
        }
        //构建系统标记
        int sysFlag = PullSysFlag.buildSysFlag(false, block, true, false, true);
        //超时时间
        long timeoutMillis = block ? this.defaultLitePullConsumer.getConsumerTimeoutMillisWhenSuspend() : timeout;
        //是否是tag过滤
        boolean isTagType = ExpressionType.isTagType(subscriptionData.getExpressionType());
        //拉取消息
        PullResult pullResult = this.pullAPIWrapper.pullKernelImpl(
            mq,
            subscriptionData.getSubString(),
            subscriptionData.getExpressionType(),
            isTagType ? 0L : subscriptionData.getSubVersion(),
            offset,
            maxNums,
            sysFlag,
            0,
            this.defaultLitePullConsumer.getBrokerSuspendMaxTimeMillis(),
            timeoutMillis,
            CommunicationMode.SYNC,
            null
        );
        this.pullAPIWrapper.processPullResult(mq, pullResult, subscriptionData);
        return pullResult;
    }

    /**
     * 重置消息的topic 去掉 对应的 namespace
     * @param msgList
     */
    private void resetTopic(List<MessageExt> msgList) {
        if (null == msgList || msgList.size() == 0) {
            return;
        }

        //If namespace not null , reset Topic without namespace.
        for (MessageExt messageExt : msgList) {
            if (null != this.defaultLitePullConsumer.getNamespace()) {
                messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(), this.defaultLitePullConsumer.getNamespace()));
            }
        }

    }

    /**
     * 更新消息队列的偏移量
     * @param mq
     * @param offset
     */
    public void updateConsumeOffset(MessageQueue mq, long offset) {
        checkServiceState();
        this.offsetStore.updateOffset(mq, offset, false);
    }

    @Override
    public String groupName() {
        return this.defaultLitePullConsumer.getConsumerGroup();
    }

    @Override
    public MessageModel messageModel() {
        return this.defaultLitePullConsumer.getMessageModel();
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_ACTIVELY;
    }

    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
    }

    @Override
    public Set<SubscriptionData> subscriptions() {
        Set<SubscriptionData> subSet = new HashSet<SubscriptionData>();

        subSet.addAll(this.rebalanceImpl.getSubscriptionInner().values());

        return subSet;
    }

    @Override
    public void doRebalance() {
        if (this.rebalanceImpl != null) {
            this.rebalanceImpl.doRebalance(false);
        }
    }

    /**
     * 如果订阅方式是 SUBSCRIBE 则 从 rebalanceImpl 获取 消息队列 集合
     * 如果订阅方式是 ASSIGN 则 从 assignedMessageQueue 获取 消息队列 集合 进行持久化
     */
    @Override
    public void persistConsumerOffset() {
        try {
            checkServiceState();
            Set<MessageQueue> mqs = new HashSet<MessageQueue>();
            //如果订阅方式是 SUBSCRIBE 则 从 rebalanceImpl 获取 消息队列 集合
            if (this.subscriptionType == SubscriptionType.SUBSCRIBE) {
                Set<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().keySet();
                mqs.addAll(allocateMq);
            } else if (this.subscriptionType == SubscriptionType.ASSIGN) {
                //如果订阅方式是 ASSIGN 则 从 assignedMessageQueue 获取 消息队列 集合
                Set<MessageQueue> assignedMessageQueue = this.assignedMessageQueue.getAssignedMessageQueues();
                mqs.addAll(assignedMessageQueue);
            }
            //进行持久化
            this.offsetStore.persistAll(mqs);
        } catch (Exception e) {
            log.error("Persist consumer offset error for group: {} ", this.defaultLitePullConsumer.getConsumerGroup(), e);
        }
    }

    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
        Map<String, SubscriptionData> subTable = this.rebalanceImpl.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                this.rebalanceImpl.getTopicSubscribeInfoTable().put(topic, info);
            }
        }
    }

    @Override
    public boolean isSubscribeTopicNeedUpdate(String topic) {
        Map<String, SubscriptionData> subTable = this.rebalanceImpl.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
            }
        }

        return false;
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultLitePullConsumer.isUnitMode();
    }

    @Override
    public ConsumerRunningInfo consumerRunningInfo() {
        ConsumerRunningInfo info = new ConsumerRunningInfo();

        Properties prop = MixAll.object2Properties(this.defaultLitePullConsumer);
        prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, String.valueOf(this.consumerStartTimestamp));
        info.setProperties(prop);

        info.getSubscriptionSet().addAll(this.subscriptions());
        return info;
    }

    private void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException {
        this.offsetStore.updateConsumeOffsetToBroker(mq, offset, isOneway);
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public DefaultLitePullConsumer getDefaultLitePullConsumer() {
        return defaultLitePullConsumer;
    }

    public Set<MessageQueue> fetchMessageQueues(String topic) throws MQClientException {
        checkServiceState();
        //获取该topic 对应的 消息队列
        Set<MessageQueue> result = this.mQClientFactory.getMQAdminImpl().fetchSubscribeMessageQueues(topic);
        return parseMessageQueues(result);
    }

    /**
     * 遍历 topicMessageQueueChangeListenerMap 如果 该 topic 的消息队列 发生
     * @throws MQClientException
     */
    private synchronized void fetchTopicMessageQueuesAndCompare() throws MQClientException {
        for (Map.Entry<String, TopicMessageQueueChangeListener> entry : topicMessageQueueChangeListenerMap.entrySet()) {
            String topic = entry.getKey();
            TopicMessageQueueChangeListener topicMessageQueueChangeListener = entry.getValue();
            //获取该topic 旧的 消息 队列
            Set<MessageQueue> oldMessageQueues = messageQueuesForTopic.get(topic);
            //获取该topic 新的 消息 队列
            Set<MessageQueue> newMessageQueues = fetchMessageQueues(topic);
            //如果发生改变 则通知 topicMessageQueueChangeListener
            boolean isChanged = !isSetEqual(newMessageQueues, oldMessageQueues);
            if (isChanged) {
                messageQueuesForTopic.put(topic, newMessageQueues);
                if (topicMessageQueueChangeListener != null) {
                    topicMessageQueueChangeListener.onChanged(topic, newMessageQueues);
                }
            }
        }
    }

    /**
     * 判断两个消息队列集合是否一致
     * @param set1
     * @param set2
     * @return
     */
    private boolean isSetEqual(Set<MessageQueue> set1, Set<MessageQueue> set2) {
        if (set1 == null && set2 == null) {
            return true;
        }

        if (set1 == null || set2 == null || set1.size() != set2.size()
            || set1.size() == 0 || set2.size() == 0) {
            return false;
        }

        Iterator iter = set2.iterator();
        boolean isEqual = true;
        while (iter.hasNext()) {
            if (!set1.contains(iter.next())) {
                isEqual = false;
            }
        }
        return isEqual;
    }

    public AssignedMessageQueue getAssignedMessageQueue() {
        return assignedMessageQueue;
    }

    public synchronized void registerTopicMessageQueueChangeListener(String topic,
                                                                     TopicMessageQueueChangeListener listener) throws MQClientException {
        if (topic == null || listener == null) {
            throw new MQClientException("Topic or listener is null", null);
        }
        if (topicMessageQueueChangeListenerMap.containsKey(topic)) {
            log.warn("Topic {} had been registered, new listener will overwrite the old one", topic);
        }
        topicMessageQueueChangeListenerMap.put(topic, listener);
        //获取该topic 消息 对列 然后进行映射
        if (this.serviceState == ServiceState.RUNNING) {
            Set<MessageQueue> messageQueues = fetchMessageQueues(topic);
            messageQueuesForTopic.put(topic, messageQueues);
        }
    }

    private Set<MessageQueue> parseMessageQueues(Set<MessageQueue> queueSet) {
        Set<MessageQueue> resultQueues = new HashSet<MessageQueue>();
        for (MessageQueue messageQueue : queueSet) {
            String userTopic = NamespaceUtil.withoutNamespace(messageQueue.getTopic(),
                this.defaultLitePullConsumer.getNamespace());
            resultQueues.add(new MessageQueue(userTopic, messageQueue.getBrokerName(), messageQueue.getQueueId()));
        }
        return resultQueues;
    }

    public class ConsumeRequest {
        /**
         * 消息集合
         */
        private final List<MessageExt> messageExts;
        /**
         * 消息队列
         */
        private final MessageQueue messageQueue;
        /**
         * 处理队列
         */
        private final ProcessQueue processQueue;

        public ConsumeRequest(final List<MessageExt> messageExts, final MessageQueue messageQueue,
            final ProcessQueue processQueue) {
            this.messageExts = messageExts;
            this.messageQueue = messageQueue;
            this.processQueue = processQueue;
        }

        public List<MessageExt> getMessageExts() {
            return messageExts;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

    }

    public void setPullTimeDelayMillsWhenException(long pullTimeDelayMillsWhenException) {
        this.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
    }
}
