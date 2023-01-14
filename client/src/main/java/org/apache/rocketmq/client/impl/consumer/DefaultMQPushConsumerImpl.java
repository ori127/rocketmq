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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.consumer.AckCallback;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.AckStatus;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.PopCallback;
import org.apache.rocketmq.client.consumer.PopResult;
import org.apache.rocketmq.client.consumer.PopStatus;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ConsumeStatus;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.PopProcessQueueInfo;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.common.protocol.header.AckMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.ChangeInvisibleTimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;


public class DefaultMQPushConsumerImpl implements MQConsumerInner {
    /**
     * 当发生异常的时候 延迟拉取时间
     * Delay some time when exception occur
     */
    private long pullTimeDelayMillsWhenException = 3000;
    /**
     * 当被限流时 (等待ack数量超过阈值 ) 获取消息延迟时间
     * Flow control interval
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50;
    /**
     * 当暂停 延迟拉取时间
     * Delay some time when suspend pull service
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_SUSPEND = 1000;
    private static final long BROKER_SUSPEND_MAX_TIME_MILLIS = 1000 * 15;
    private static final long CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 1000 * 30;
    private final InternalLogger log = ClientLogger.getLog();
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final RebalanceImpl rebalanceImpl = new RebalancePushImpl(this);
    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();
    private final long consumerStartTimestamp = System.currentTimeMillis();
    /**
     * 消费消息的钩子
     */
    private final ArrayList<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();
    /**
     * Rpc请求钩子
     */
    private final RPCHook rpcHook;
    /**
     * 服务状态
     */
    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientInstance mQClientFactory;
    private PullAPIWrapper pullAPIWrapper;
    private volatile boolean pause = false;
    /**
     * 是否顺序消费
     */
    private boolean consumeOrderly = false;
    private MessageListener messageListenerInner;
    /**
     * 偏移量存储 若是广播模式 LocalFileOffsetStore 如果是集群模式 RemoteBrokerOffsetStore
     */
    private OffsetStore offsetStore;
    private ConsumeMessageService consumeMessageService;
    private ConsumeMessageService consumeMessagePopService;
    /**
     * 队列限流控制计数
     */
    private long queueFlowControlTimes = 0;
    /**
     * 队列消息的最大跨度 阈值
     */
    private long queueMaxSpanFlowControlTimes = 0;
    /**
     * pop 延迟时间 等级表
     */
    //10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
    private int[] popDelayLevel = new int[] {10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600, 7200};
    /**
     * 消息最大不可见时间
     */

    private static final int MAX_POP_INVISIBLE_TIME = 300000;
    /**
     * 消息最小不可见时间
     */
    private static final int MIN_POP_INVISIBLE_TIME = 5000;
    /**
     * 异步超时时间
     */
    private static final int ASYNC_TIMEOUT = 3000;

    // only for test purpose, will be modified by reflection in unit test.
    @SuppressWarnings("FieldMayBeFinal") private static boolean doNotUpdateTopicSubscribeInfoWhenSubscriptionChanged = false;

    public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer, RPCHook rpcHook) {
        this.defaultMQPushConsumer = defaultMQPushConsumer;
        this.rpcHook = rpcHook;
        this.pullTimeDelayMillsWhenException = defaultMQPushConsumer.getPullTimeDelayMillsWhenException();
    }

    /**
     * 注册过滤消息的钩子
     * @param hook
     */
    public void registerFilterMessageHook(final FilterMessageHook hook) {
        this.filterMessageHookList.add(hook);
        log.info("register FilterMessageHook Hook, {}", hook.hookName());
    }

    /**
     * 是否存在消费消息的钩子
     * @return
     */
    public boolean hasHook() {
        return !this.consumeMessageHookList.isEmpty();
    }

    /**
     * 注册消费消息的钩子
     * @param hook
     */
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
                }
            }
        }
    }

    /**
     * 根据key 获取该 key 的 BrokerData 遍历创建 topic
     * @param key
     * @param newTopic
     * @param queueNum
     * @throws MQClientException
     */
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    /**
     * 根据key 获取该 key 的 BrokerData 遍历创建 topic
     * @param key
     * @param newTopic
     * @param queueNum
     * @param topicSysFlag
     * @throws MQClientException
     */
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag, null);
    }

    /**
     * 获取该 topic 的  MessageQueue 没有则从 nameServer拉取路由信息
     * @param topic
     * @return
     * @throws MQClientException
     */
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        //获取topic 的 MessageQueue
        //若果没有 则从 nameServer拉取路由信息
        //重新获取 topic信息
        Set<MessageQueue> result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        if (null == result) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        }

        if (null == result) {
            throw new MQClientException("The topic[" + topic + "] not exist", null);
        }
        //遍历 messageQueueList 去掉 namespace
        return parseSubscribeMessageQueues(result);
    }

    /**
     * 遍历 messageQueueList 去掉 namespace
     * @param messageQueueList
     * @return
     */
    public Set<MessageQueue> parseSubscribeMessageQueues(Set<MessageQueue> messageQueueList) {
        //遍历messageQueueList 去掉 namespace
        Set<MessageQueue> resultQueues = new HashSet<MessageQueue>();
        for (MessageQueue queue : messageQueueList) {
            String userTopic = NamespaceUtil.withoutNamespace(queue.getTopic(), this.defaultMQPushConsumer.getNamespace());
            resultQueues.add(new MessageQueue(userTopic, queue.getBrokerName(), queue.getQueueId()));
        }

        return resultQueues;
    }

    public DefaultMQPushConsumer getDefaultMQPushConsumer() {
        return defaultMQPushConsumer;
    }

    /**
     *  获取MessageQueue 消息最早 存储时间
     * @param mq
     * @return
     * @throws MQClientException
     */
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }

    /**
     * 查询给定消息队列的最大偏移量
     * @param mq
     * @return
     * @throws MQClientException
     */
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }

    /**
     * 查询给定消息队列的最小偏移量。
     * @param mq
     * @return
     * @throws MQClientException
     */
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    public void pullMessage(final PullRequest pullRequest) {
        final ProcessQueue processQueue = pullRequest.getProcessQueue();
        if (processQueue.isDropped()) {
            log.info("the pull request[{}] is dropped.", pullRequest.toString());
            return;
        }
        //更新处理队列的获取消息时间
        pullRequest.getProcessQueue().setLastPullTimestamp(System.currentTimeMillis());

        try {
            this.makeSureStateOK();
        } catch (MQClientException e) {
            log.warn("pullMessage exception, consumer state not ok", e);
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            return;
        }

        if (this.isPause()) {
            log.warn("consumer was paused, execute pull request later. instanceName={}, group={}", this.defaultMQPushConsumer.getInstanceName(), this.defaultMQPushConsumer.getConsumerGroup());
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
            return;
        }
        //获取消息总数量
        long cachedMessageCount = processQueue.getMsgCount().get();
        //获取消息总大小
        long cachedMessageSizeInMiB = processQueue.getMsgSize().get() / (1024 * 1024);
        //超过消费数量阈值 增加限流计数 FIXME?? 超过阈值重新 请求
        if (cachedMessageCount > this.defaultMQPushConsumer.getPullThresholdForQueue()) {
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
            if ((queueFlowControlTimes++ % 1000) == 0) {
                log.warn(
                    "the cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                    this.defaultMQPushConsumer.getPullThresholdForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
            }
            return;
        }
        //超过消费大小阈值  增加限流计数 FIXME?? 超过阈值重新 请求
        if (cachedMessageSizeInMiB > this.defaultMQPushConsumer.getPullThresholdSizeForQueue()) {
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
            if ((queueFlowControlTimes++ % 1000) == 0) {
                log.warn(
                    "the cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                    this.defaultMQPushConsumer.getPullThresholdSizeForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
            }
            return;
        }

        //不是顺序消费 增加限流计数 FIXME?? 超过阈值重新 请求
        if (!this.consumeOrderly) {
            if (processQueue.getMaxSpan() > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()) {
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
                if ((queueMaxSpanFlowControlTimes++ % 1000) == 0) {
                    log.warn(
                        "the queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, pullRequest={}, flowControlTimes={}",
                        processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), processQueue.getMaxSpan(),
                        pullRequest, queueMaxSpanFlowControlTimes);
                }
                return;
            }
        } else {
            //顺序消费 已经上锁
            if (processQueue.isLocked()) {
                //不是已经提前上锁 根据消息队列 获取 偏移量
                if (!pullRequest.isPreviouslyLocked()) {
                    long offset = -1L;
                    try {
                        //根据消息队列 获取 偏移量
                        offset = this.rebalanceImpl.computePullFromWhereWithException(pullRequest.getMessageQueue());
                        if (offset < 0) {
                            throw new MQClientException(ResponseCode.SYSTEM_ERROR, "Unexpected offset " + offset);
                        }
                    } catch (Exception e) {
                        this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                        log.error("Failed to compute pull offset, pullResult: {}", pullRequest, e);
                        return;
                    }
                    boolean brokerBusy = offset < pullRequest.getNextOffset();
                    log.info("the first time to pull message, so fix offset from broker. pullRequest: {} NewOffset: {} brokerBusy: {}",
                        pullRequest, offset, brokerBusy);
                    if (brokerBusy) {
                        log.info("[NOTIFYME]the first time to pull message, but pull request offset larger than broker consume offset. pullRequest: {} NewOffset: {}",
                            pullRequest, offset);
                    }
                    //提交前上锁 设置偏移量
                    pullRequest.setPreviouslyLocked(true);
                    pullRequest.setNextOffset(offset);
                }
            } else {
                //延迟获取消息
                this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                log.info("pull message later because not locked in broker, {}", pullRequest);
                return;
            }
        }
        //获取 该  topic 订阅关系 若果没有订阅关系  延迟获取消息
        final SubscriptionData subscriptionData = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (null == subscriptionData) {
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            log.warn("find the consumer's subscription failed, {}", pullRequest);
            return;
        }

        final long beginTimestamp = System.currentTimeMillis();

        PullCallback pullCallback = new PullCallback() {
            @Override
            public void onSuccess(PullResult pullResult) {
                if (pullResult != null) {
                    //对消息进行解码 过滤
                    pullResult = DefaultMQPushConsumerImpl.this.pullAPIWrapper.processPullResult(pullRequest.getMessageQueue(), pullResult,
                        subscriptionData);

                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            //之前的偏移量
                            long prevRequestOffset = pullRequest.getNextOffset();
                            //设置下次获取消息开始的偏移量
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                            //记录获取消息所花费的时间
                            long pullRT = System.currentTimeMillis() - beginTimestamp;
                            DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullRT(pullRequest.getConsumerGroup(),
                                pullRequest.getMessageQueue().getTopic(), pullRT);

                            long firstMsgOffset = Long.MAX_VALUE;
                            //若果获取的消息 为空 则 提交 获取立即消息的请求
                            if (pullResult.getMsgFoundList() == null || pullResult.getMsgFoundList().isEmpty()) {
                                DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            } else {
                                //第一个消息偏移量
                                firstMsgOffset = pullResult.getMsgFoundList().get(0).getQueueOffset();
                                //记录获取消息的数量
                                DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullTPS(pullRequest.getConsumerGroup(),
                                    pullRequest.getMessageQueue().getTopic(), pullResult.getMsgFoundList().size());
                                //将消息放置 到对应 处理队列
                                boolean dispatchToConsume = processQueue.putMessage(pullResult.getMsgFoundList());
                                //提交消费消息请求 消费消息
                                DefaultMQPushConsumerImpl.this.consumeMessageService.submitConsumeRequest(
                                    pullResult.getMsgFoundList(),
                                    processQueue,
                                    pullRequest.getMessageQueue(),
                                    dispatchToConsume);
                                //根据获取消息间隔 提交获取消息请求
                                if (DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval() > 0) {
                                    DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest,
                                        DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());
                                } else {
                                    DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                                }
                            }

                            if (pullResult.getNextBeginOffset() < prevRequestOffset
                                || firstMsgOffset < prevRequestOffset) {
                                log.warn(
                                    "[BUG] pull message result maybe data wrong, nextBeginOffset: {} firstMsgOffset: {} prevRequestOffset: {}",
                                    pullResult.getNextBeginOffset(),
                                    firstMsgOffset,
                                    prevRequestOffset);
                            }

                            break;
                        case NO_NEW_MSG:
                        case NO_MATCHED_MSG:
                            //没有消息 更新存储消息偏移量 提交获取消息请求
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                            DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);

                            DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            break;
                        case OFFSET_ILLEGAL:
                            //非法的偏移量
                            log.warn("the pull request offset illegal, {} {}",
                                pullRequest.toString(), pullResult.toString());
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                            //非法偏移量 就将该处理队列标记丢弃
                            pullRequest.getProcessQueue().setDropped(true);
                            //存储该位置偏移量 进行持久化 然后移除该消息队列的处理队列
                            DefaultMQPushConsumerImpl.this.executeTaskLater(new Runnable() {

                                @Override
                                public void run() {
                                    try {
                                        DefaultMQPushConsumerImpl.this.offsetStore.updateOffset(pullRequest.getMessageQueue(),
                                            pullRequest.getNextOffset(), false);

                                        DefaultMQPushConsumerImpl.this.offsetStore.persist(pullRequest.getMessageQueue());
                                        //移除该处理队列
                                        DefaultMQPushConsumerImpl.this.rebalanceImpl.removeProcessQueue(pullRequest.getMessageQueue());

                                        log.warn("fix the pull request offset, {}", pullRequest);
                                    } catch (Throwable e) {
                                        log.error("executeTaskLater Exception", e);
                                    }
                                }
                            }, 10000);
                            break;
                        default:
                            break;
                    }
                }
            }

            @Override
            public void onException(Throwable e) {
                //发生异常 则延迟获取消息
                if (!pullRequest.getMessageQueue().getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    log.warn("execute the pull request exception", e);
                }

                DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            }
        };

        boolean commitOffsetEnable = false;
        long commitOffsetValue = 0L;
        //如果是消费者是集群模式则从 内存读取 偏移量
        if (MessageModel.CLUSTERING == this.defaultMQPushConsumer.getMessageModel()) {
            commitOffsetValue = this.offsetStore.readOffset(pullRequest.getMessageQueue(), ReadOffsetType.READ_FROM_MEMORY);
            if (commitOffsetValue > 0) {
                commitOffsetEnable = true;
            }
        }
        //订阅表达式
        String subExpression = null;
        //是否类过滤
        boolean classFilter = false;
        //根据topic 获取 订阅关系
        SubscriptionData sd = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (sd != null) {
            if (this.defaultMQPushConsumer.isPostSubscriptionWhenPull() && !sd.isClassFilterMode()) {
                subExpression = sd.getSubString();
            }

            classFilter = sd.isClassFilterMode();
        }
        //构建系统标记
        int sysFlag = PullSysFlag.buildSysFlag(
            commitOffsetEnable, // commitOffset
            true, // suspend
            subExpression != null, // subscription
            classFilter // class filter
        );
        try {
            //获取消息
            this.pullAPIWrapper.pullKernelImpl(
                pullRequest.getMessageQueue(),
                subExpression,
                subscriptionData.getExpressionType(),
                subscriptionData.getSubVersion(),
                pullRequest.getNextOffset(),
                this.defaultMQPushConsumer.getPullBatchSize(),
                this.defaultMQPushConsumer.getPullBatchSizeInBytes(),
                sysFlag,
                commitOffsetValue,
                BROKER_SUSPEND_MAX_TIME_MILLIS,
                CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND,
                CommunicationMode.ASYNC,
                pullCallback
            );
        } catch (Exception e) {
            log.error("pullKernelImpl exception", e);
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
        }
    }

    void popMessage(final PopRequest popRequest) {
        //获取消息的处理队列 更新 pop 时间戳
        final PopProcessQueue processQueue = popRequest.getPopProcessQueue();
        if (processQueue.isDropped()) {
            log.info("the pop request[{}] is dropped.", popRequest.toString());
            return;
        }

        processQueue.setLastPopTimestamp(System.currentTimeMillis());

        try {
            this.makeSureStateOK();
        } catch (MQClientException e) {
            log.warn("pullMessage exception, consumer state not ok", e);
            //发生异常 执行延迟3秒获取消息时间
            this.executePopPullRequestLater(popRequest, pullTimeDelayMillsWhenException);
            return;
        }
        //当暂停 延迟1秒时间拉取消息
        if (this.isPause()) {
            log.warn("consumer was paused, execute pull request later. instanceName={}, group={}", this.defaultMQPushConsumer.getInstanceName(), this.defaultMQPushConsumer.getConsumerGroup());
            this.executePopPullRequestLater(popRequest, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
            return;
        }
        //等待ack确认的数量超过 最大等待 ack 确认的消息数量 阈值 延迟50毫秒时间拉取消息
        if (processQueue.getWaiAckMsgCount() > this.defaultMQPushConsumer.getPopThresholdForQueue()) {
            this.executePopPullRequestLater(popRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
            if ((queueFlowControlTimes++ % 1000) == 0) {
                log.warn("the messages waiting to ack exceeds the threshold {}, so do flow control, popRequest={}, flowControlTimes={}, wait count={}",
                    this.defaultMQPushConsumer.getPopThresholdForQueue(), popRequest, queueFlowControlTimes, processQueue.getWaiAckMsgCount());
            }
            return;
        }

        //POPTODO think of pop mode orderly implementation later.
        //根据拉取消息 topic 获取订阅信息
        final SubscriptionData subscriptionData = this.rebalanceImpl.getSubscriptionInner().get(popRequest.getMessageQueue().getTopic());
        //如果订阅消息为 null 执行延迟3秒获取消息时间
        if (null == subscriptionData) {
            this.executePopPullRequestLater(popRequest, pullTimeDelayMillsWhenException);
            log.warn("find the consumer's subscription failed, {}", popRequest);
            return;
        }
        //计算开始时间
        final long beginTimestamp = System.currentTimeMillis();

        PopCallback popCallback = new PopCallback() {
            @Override
            public void onSuccess(PopResult popResult) {
                //如果没有结果则 重新提交获取消息请求
                if (popResult == null) {
                    log.error("pop callback popResult is null");
                    DefaultMQPushConsumerImpl.this.executePopPullRequestImmediately(popRequest);
                    return;
                }
                //根据订阅信息 过滤消息
                processPopResult(popResult, subscriptionData);

                switch (popResult.getPopStatus()) {
                    case FOUND:
                        //计算拉取响应时间
                        long pullRT = System.currentTimeMillis() - beginTimestamp;
                        //统计获取响应时间
                        DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullRT(popRequest.getConsumerGroup(),
                            popRequest.getMessageQueue().getTopic(), pullRT);
                        if (popResult.getMsgFoundList() == null || popResult.getMsgFoundList().isEmpty()) {
                            DefaultMQPushConsumerImpl.this.executePopPullRequestImmediately(popRequest);
                        } else {
                            //统计获取的tps
                            DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullTPS(popRequest.getConsumerGroup(),
                                popRequest.getMessageQueue().getTopic(), popResult.getMsgFoundList().size());
                            popRequest.getPopProcessQueue().incFoundMsg(popResult.getMsgFoundList().size());

                            DefaultMQPushConsumerImpl.this.consumeMessagePopService.submitPopConsumeRequest(
                                popResult.getMsgFoundList(),
                                processQueue,
                                popRequest.getMessageQueue());
                            //重新提交拉取请求
                            if (DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval() > 0) {
                                DefaultMQPushConsumerImpl.this.executePopPullRequestLater(popRequest,
                                    DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());
                            } else {
                                DefaultMQPushConsumerImpl.this.executePopPullRequestImmediately(popRequest);
                            }
                        }
                        break;
                    // 轮训时间到了 但是没有消息 或者 轮训时间结束没有获取新消息 投递请求到线程池
                    case NO_NEW_MSG:
                    case POLLING_NOT_FOUND:
                        DefaultMQPushConsumerImpl.this.executePopPullRequestImmediately(popRequest);
                        break;
                        // 轮训池已经满了 延迟3秒获取
                    case POLLING_FULL:
                        DefaultMQPushConsumerImpl.this.executePopPullRequestLater(popRequest, pullTimeDelayMillsWhenException);
                        break;
                    default:
                        DefaultMQPushConsumerImpl.this.executePopPullRequestLater(popRequest, pullTimeDelayMillsWhenException);
                        break;
                }

            }

            @Override
            public void onException(Throwable e) {
                if (!popRequest.getMessageQueue().getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    log.warn("execute the pull request exception: {}", e);
                }

                DefaultMQPushConsumerImpl.this.executePopPullRequestLater(popRequest, pullTimeDelayMillsWhenException);
            }
        };


        try {
            //获取消息不见时间
            long invisibleTime = this.defaultMQPushConsumer.getPopInvisibleTime();
            if (invisibleTime < MIN_POP_INVISIBLE_TIME || invisibleTime > MAX_POP_INVISIBLE_TIME) {
                invisibleTime = 60000;
            }
            //异步pop
            this.pullAPIWrapper.popAsync(popRequest.getMessageQueue(), invisibleTime, this.defaultMQPushConsumer.getPopBatchNums(),
                popRequest.getConsumerGroup(), BROKER_SUSPEND_MAX_TIME_MILLIS, popCallback, true, popRequest.getInitMode(),
                false, subscriptionData.getExpressionType(), subscriptionData.getSubString());
        } catch (Exception e) {
            log.error("popAsync exception", e);
            this.executePopPullRequestLater(popRequest, pullTimeDelayMillsWhenException);
        }
    }

    /**
     * 根据订阅信息 过滤消息
     * @param popResult
     * @param subscriptionData
     * @return
     */
    private PopResult processPopResult(final PopResult popResult, final SubscriptionData subscriptionData) {
        //有消息
        if (PopStatus.FOUND == popResult.getPopStatus()) {
            List<MessageExt> msgFoundList = popResult.getMsgFoundList();
            List<MessageExt> msgListFilterAgain = msgFoundList;
            //订阅信息 tags 不为空 并且 订阅模式 不是 class 过滤
            if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()
                && popResult.getMsgFoundList().size() > 0) {
                msgListFilterAgain = new ArrayList<MessageExt>(popResult.getMsgFoundList().size());
                //遍历消息判断 该 tags 是否包含 tags 根据 tags 来过滤
                for (MessageExt msg : popResult.getMsgFoundList()) {
                    if (msg.getTags() != null) {
                        if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                            msgListFilterAgain.add(msg);
                        }
                    }
                }
            }
            //过滤消息钩子 不为空 则 遍历 执行消息 钩子
            if (!this.filterMessageHookList.isEmpty()) {
                FilterMessageContext filterMessageContext = new FilterMessageContext();
                filterMessageContext.setUnitMode(this.defaultMQPushConsumer.isUnitMode());
                filterMessageContext.setMsgList(msgListFilterAgain);
                if (!this.filterMessageHookList.isEmpty()) {
                    for (FilterMessageHook hook : this.filterMessageHookList) {
                        try {
                            hook.filterMessage(filterMessageContext);
                        } catch (Throwable e) {
                            log.error("execute hook error. hookName={}", hook.hookName());
                        }
                    }
                }
            }
            //如果获取的消息数量 和 过滤出来的消息数量不一致
            //FIXME::那么将 不满足过滤条件的消息 异步确认
            if (msgFoundList.size() != msgListFilterAgain.size()) {
                for (MessageExt msg : msgFoundList) {
                    if (!msgListFilterAgain.contains(msg)) {
                        ackAsync(msg, this.groupName());
                    }
                }
            }

            popResult.setMsgFoundList(msgListFilterAgain);
        }

        return popResult;
    }

    /**
     * 确保服务是运行中的
     * @throws MQClientException
     */
    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The consumer service state not OK, "
                + this.serviceState
                + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                null);
        }
    }

    /**
     * 提交获取消费请求
     * @param pullRequest
     * @param timeDelay
     */
    void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executePullRequestLater(pullRequest, timeDelay);
    }

    public boolean isPause() {
        return pause;
    }

    public void setPause(boolean pause) {
        this.pause = pause;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.mQClientFactory.getConsumerStatsManager();
    }

    /**
     *提交 获取立即消息的请求
     * @param pullRequest
     */
    public void executePullRequestImmediately(final PullRequest pullRequest) {
        this.mQClientFactory.getPullMessageService().executePullRequestImmediately(pullRequest);
    }

    /**
     * 执行延迟获取消息时间
     * @param pullRequest
     * @param timeDelay
     */
    void executePopPullRequestLater(final PopRequest pullRequest, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executePopPullRequestLater(pullRequest, timeDelay);
    }

    void executePopPullRequestImmediately(final PopRequest pullRequest) {
        this.mQClientFactory.getPullMessageService().executePopPullRequestImmediately(pullRequest);
    }

    /**
     * 更新获取 消息的偏移量
     * @param pullRequest
     */
    private void correctTagsOffset(final PullRequest pullRequest) {
        if (0L == pullRequest.getProcessQueue().getMsgCount().get()) {
            this.offsetStore.updateOffset(pullRequest.getMessageQueue(), pullRequest.getNextOffset(), true);
        }
    }

    public void executeTaskLater(final Runnable r, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executeTaskLater(r, timeDelay);
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    public MessageExt queryMessageByUniqKey(String topic, String uniqKey) throws MQClientException,
        InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
    }

    public void registerMessageListener(MessageListener messageListener) {
        this.messageListenerInner = messageListener;
    }

    public void resume() {
        this.pause = false;
        doRebalance();
        log.info("resume this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }

    @Deprecated
    public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        sendMessageBack(msg, delayLevel, brokerName, null);
    }

    public void sendMessageBack(MessageExt msg, int delayLevel, final MessageQueue mq)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        sendMessageBack(msg, delayLevel, null, mq);
    }


    private void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName, final MessageQueue mq)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            //如果 brokerName 以 __syslo__ 开头 或者 消息队列的 brokerName  以 __syslo__ 开头
            if (brokerName != null && brokerName.startsWith(MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX)
                || mq != null && mq.getBrokerName().startsWith(MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX)) {
                //发回消息
                sendMessageBackAsNormalMessage(msg);
            } else {
                //根据 brokerName 找到 对应  brokerAddr 根据存储的 host 找对应的 broker地址
                String brokerAddr = (null != brokerName) ? this.mQClientFactory.findBrokerAddressInPublish(brokerName)
                        : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());
                //消费者回发消息
                this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(brokerAddr, msg,
                        this.defaultMQPushConsumer.getConsumerGroup(), delayLevel, 5000, getMaxReconsumeTimes());
            }
        } catch (Exception e) {
            log.error("sendMessageBack Exception, " + this.defaultMQPushConsumer.getConsumerGroup(), e);
            sendMessageBackAsNormalMessage(msg);
        } finally {
            msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
        }
    }

    /**
     * 发回消息
     * @param msg
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    private void sendMessageBackAsNormalMessage(MessageExt msg) throws  RemotingException, MQBrokerException, InterruptedException, MQClientException {
        Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());

        String originMsgId = MessageAccessor.getOriginMessageId(msg);
        MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);

        newMsg.setFlag(msg.getFlag());
        MessageAccessor.setProperties(newMsg, msg.getProperties());
        MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
        MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes() + 1));
        MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
        MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);
        //FIXME::为什么延迟等级都+3
        newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

        this.mQClientFactory.getDefaultMQProducer().send(newMsg);
    }

    /**
     * 异步消息确认
     * @param message
     * @param consumerGroup
     */
    void ackAsync(MessageExt message, String consumerGroup) {
        //获取 POP_CK
        final String extraInfo = message.getProperty(MessageConst.PROPERTY_POP_CK);

        try {
            String[] extraInfoStrs = ExtraInfoUtil.split(extraInfo);
            //获取 brokerName  extraInfoStrs[5] 为 brokerName
            String brokerName = ExtraInfoUtil.getBrokerName(extraInfoStrs);
            //获取 queueId extraInfoStrs[6] 为 QueueId
            int queueId = ExtraInfoUtil.getQueueId(extraInfoStrs);
            //获取 queueOffset extraInfoStrs[6] 为 QueueId
            long queueOffset = ExtraInfoUtil.getQueueOffset(extraInfoStrs);
            String topic = message.getTopic();

            String desBrokerName = brokerName;
            //brokerName 以 __syslo__ 为前缀 则 topic 添加 namespace FIXME:: 获取 desBrokerName ?
            if (brokerName != null && brokerName.startsWith(MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX)) {
                desBrokerName = this.mQClientFactory.getBrokerNameFromMessageQueue(this.defaultMQPushConsumer.queueWithNamespace(new MessageQueue(topic, brokerName, queueId)));
            }

            //根据 broker 和 brokerId 获取 该 broker信息
            FindBrokerResult
                findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(desBrokerName, MixAll.MASTER_ID, true);
            //如果 broker 信息 不存在 则 重新 从 nameServer 获取路由信息
            if (null == findBrokerResult) {
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
                findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(desBrokerName, MixAll.MASTER_ID, true);
            }

            if (findBrokerResult == null) {
                log.error("The broker[" + desBrokerName + "] not exist");
                return;
            }

            AckMessageRequestHeader requestHeader = new AckMessageRequestHeader();
            requestHeader.setTopic(ExtraInfoUtil.getRealTopic(extraInfoStrs, topic, consumerGroup));
            requestHeader.setQueueId(queueId);
            requestHeader.setOffset(queueOffset);
            requestHeader.setConsumerGroup(consumerGroup);
            requestHeader.setExtraInfo(extraInfo);
            //发送异步发送ack去确认消息
            this.mQClientFactory.getMQClientAPIImpl().ackMessageAsync(findBrokerResult.getBrokerAddr(), ASYNC_TIMEOUT, new AckCallback() {
                @Override
                public void onSuccess(AckResult ackResult) {
                    if (ackResult != null && !AckStatus.OK.equals(ackResult.getStatus())) {
                        log.warn("Ack message fail. ackResult: {}, extraInfo: {}", ackResult, extraInfo);
                    }
                }
                @Override
                public void onException(Throwable e) {
                    log.warn("Ack message fail. extraInfo: {}  error message: {}", extraInfo, e.toString());
                }
            }, requestHeader);

        } catch (Throwable t) {
            log.error("ack async error.", t);
        }
    }

    /**
     * 更改消息不可见的时间
     * @param topic
     * @param consumerGroup
     * @param extraInfo
     * @param invisibleTime
     * @param callback
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    void changePopInvisibleTimeAsync(String topic, String consumerGroup, String extraInfo, long invisibleTime, AckCallback callback)
        throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        //根据额外信息 获取消息的 brokerName 和 消息的队列的 id
        String[] extraInfoStrs = ExtraInfoUtil.split(extraInfo);
        String brokerName = ExtraInfoUtil.getBrokerName(extraInfoStrs);
        int queueId = ExtraInfoUtil.getQueueId(extraInfoStrs);

        String desBrokerName = brokerName;
        //FIXME:: 这 __syslo__ 开头的 brokerName 是干什么的哦?
        if (brokerName != null && brokerName.startsWith(MixAll.LOGICAL_QUEUE_MOCK_BROKER_PREFIX)) {
            desBrokerName = this.mQClientFactory.getBrokerNameFromMessageQueue(this.defaultMQPushConsumer.queueWithNamespace(new MessageQueue(topic, brokerName, queueId)));
        }
        //查找对应的Broker信息
        FindBrokerResult
            findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(desBrokerName, MixAll.MASTER_ID, true);
        //更新路由 重新获取 Broker信息
        if (null == findBrokerResult) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(desBrokerName, MixAll.MASTER_ID, true);
        }
        if (findBrokerResult != null) {
            ChangeInvisibleTimeRequestHeader requestHeader = new ChangeInvisibleTimeRequestHeader();
            requestHeader.setTopic(ExtraInfoUtil.getRealTopic(extraInfoStrs, topic, consumerGroup));
            requestHeader.setQueueId(queueId);
            requestHeader.setOffset(ExtraInfoUtil.getQueueOffset(extraInfoStrs));
            requestHeader.setConsumerGroup(consumerGroup);
            requestHeader.setExtraInfo(extraInfo);
            requestHeader.setInvisibleTime(invisibleTime);
            //异步更改消息不可见的时间
            //here the broker should be polished
            this.mQClientFactory.getMQClientAPIImpl().changeInvisibleTimeAsync(brokerName, findBrokerResult.getBrokerAddr(), requestHeader, ASYNC_TIMEOUT, callback);
            return;
        }
        throw new MQClientException("The broker[" + desBrokerName + "] not exist", null);
    }

    public int getMaxReconsumeTimes() {
        // default reconsume times: 16
        if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
            return 16;
        } else {
            return this.defaultMQPushConsumer.getMaxReconsumeTimes();
        }
    }

    public void shutdown() {
        shutdown(0);
    }

    public synchronized void shutdown(long awaitTerminateMillis) {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.consumeMessageService.shutdown(awaitTerminateMillis);
                this.persistConsumerOffset();
                this.mQClientFactory.unregisterConsumer(this.defaultMQPushConsumer.getConsumerGroup());
                this.mQClientFactory.shutdown();
                log.info("the consumer [{}] shutdown OK", this.defaultMQPushConsumer.getConsumerGroup());
                this.rebalanceImpl.destroy();
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    public synchronized void start() throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                log.info("the consumer [{}] start beginning. messageModel={}, isUnitMode={}", this.defaultMQPushConsumer.getConsumerGroup(),
                    this.defaultMQPushConsumer.getMessageModel(), this.defaultMQPushConsumer.isUnitMode());
                this.serviceState = ServiceState.START_FAILED;

                this.checkConfig();
                //rebalanceImpl 拷贝订阅关系
                this.copySubscription();
                //如果是集群模式 则 将实例名称 改成 Pid#System.nanoTime()
                if (this.defaultMQPushConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                    this.defaultMQPushConsumer.changeInstanceNameToPID();
                }
                //获取 mQClientFactory
                this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQPushConsumer, this.rpcHook);
                //为 rebalanceImpl 设置 生产组  获取消息模式 获取 队列分配
                this.rebalanceImpl.setConsumerGroup(this.defaultMQPushConsumer.getConsumerGroup());
                this.rebalanceImpl.setMessageModel(this.defaultMQPushConsumer.getMessageModel());
                this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPushConsumer.getAllocateMessageQueueStrategy());
                this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);
                //实例化 PullAPIWrapper
                if (this.pullAPIWrapper == null) {
                    this.pullAPIWrapper = new PullAPIWrapper(
                        mQClientFactory,
                        this.defaultMQPushConsumer.getConsumerGroup(), isUnitMode());
                }
                //注册消息过滤钩子
                this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);

                if (this.defaultMQPushConsumer.getOffsetStore() != null) {
                    this.offsetStore = this.defaultMQPushConsumer.getOffsetStore();
                } else {
                    //根据其 获取消息模式 选择 对应 偏移量 存储 如果是 广播模式 则 是 LocalFileOffsetStore
                    //如果是集群模式 则 是 RemoteBrokerOffsetStore
                    switch (this.defaultMQPushConsumer.getMessageModel()) {
                        case BROADCASTING:
                            this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                            break;
                        case CLUSTERING:
                            this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                            break;
                        default:
                            break;
                    }
                    this.defaultMQPushConsumer.setOffsetStore(this.offsetStore);
                }
                this.offsetStore.load();
                //根据其对应消息监听 设置对应的 消费消息 服务
                if (this.getMessageListenerInner() instanceof MessageListenerOrderly) {
                    this.consumeOrderly = true;
                    this.consumeMessageService =
                        new ConsumeMessageOrderlyService(this, (MessageListenerOrderly) this.getMessageListenerInner());
                    //POPTODO reuse Executor ?
                    this.consumeMessagePopService = new ConsumeMessagePopOrderlyService(this, (MessageListenerOrderly) this.getMessageListenerInner());
                } else if (this.getMessageListenerInner() instanceof MessageListenerConcurrently) {
                    this.consumeOrderly = false;
                    this.consumeMessageService =
                        new ConsumeMessageConcurrentlyService(this, (MessageListenerConcurrently) this.getMessageListenerInner());
                    //POPTODO reuse Executor ?
                    this.consumeMessagePopService =
                        new ConsumeMessagePopConcurrentlyService(this, (MessageListenerConcurrently) this.getMessageListenerInner());
                }

                this.consumeMessageService.start();
                // POPTODO
                this.consumeMessagePopService.start();
                //注册对应的消费者
                boolean registerOK = mQClientFactory.registerConsumer(this.defaultMQPushConsumer.getConsumerGroup(), this);
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;
                    this.consumeMessageService.shutdown(defaultMQPushConsumer.getAwaitTerminationMillisWhenShutdown());
                    throw new MQClientException("The consumer group[" + this.defaultMQPushConsumer.getConsumerGroup()
                        + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                        null);
                }

                mQClientFactory.start();
                log.info("the consumer [{}] start OK.", this.defaultMQPushConsumer.getConsumerGroup());
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The PushConsumer service state not OK, maybe started once, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
            default:
                break;
        }
        //遍历订阅关系 表 然后 根据 topic 从NameServer那边 获取路由信息
        this.updateTopicSubscribeInfoWhenSubscriptionChanged();
        this.mQClientFactory.checkClientInBroker();
        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
        this.mQClientFactory.rebalanceImmediately();
    }

    private void checkConfig() throws MQClientException {
        Validators.checkGroup(this.defaultMQPushConsumer.getConsumerGroup());

        if (null == this.defaultMQPushConsumer.getConsumerGroup()) {
            throw new MQClientException(
                "consumerGroup is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        if (this.defaultMQPushConsumer.getConsumerGroup().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
            throw new MQClientException(
                "consumerGroup can not equal "
                    + MixAll.DEFAULT_CONSUMER_GROUP
                    + ", please specify another one."
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        if (null == this.defaultMQPushConsumer.getMessageModel()) {
            throw new MQClientException(
                "messageModel is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        if (null == this.defaultMQPushConsumer.getConsumeFromWhere()) {
            throw new MQClientException(
                "consumeFromWhere is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        Date dt = UtilAll.parseDate(this.defaultMQPushConsumer.getConsumeTimestamp(), UtilAll.YYYYMMDDHHMMSS);
        if (null == dt) {
            throw new MQClientException(
                "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received "
                    + this.defaultMQPushConsumer.getConsumeTimestamp()
                    + " " + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        // allocateMessageQueueStrategy
        if (null == this.defaultMQPushConsumer.getAllocateMessageQueueStrategy()) {
            throw new MQClientException(
                "allocateMessageQueueStrategy is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // subscription
        if (null == this.defaultMQPushConsumer.getSubscription()) {
            throw new MQClientException(
                "subscription is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // messageListener
        if (null == this.defaultMQPushConsumer.getMessageListener()) {
            throw new MQClientException(
                "messageListener is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        boolean orderly = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerOrderly;
        boolean concurrently = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerConcurrently;
        if (!orderly && !concurrently) {
            throw new MQClientException(
                "messageListener must be instanceof MessageListenerOrderly or MessageListenerConcurrently"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // consumeThreadMin
        if (this.defaultMQPushConsumer.getConsumeThreadMin() < 1
            || this.defaultMQPushConsumer.getConsumeThreadMin() > 1000) {
            throw new MQClientException(
                "consumeThreadMin Out of range [1, 1000]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // consumeThreadMax
        if (this.defaultMQPushConsumer.getConsumeThreadMax() < 1 || this.defaultMQPushConsumer.getConsumeThreadMax() > 1000) {
            throw new MQClientException(
                "consumeThreadMax Out of range [1, 1000]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // consumeThreadMin can't be larger than consumeThreadMax
        if (this.defaultMQPushConsumer.getConsumeThreadMin() > this.defaultMQPushConsumer.getConsumeThreadMax()) {
            throw new MQClientException(
                "consumeThreadMin (" + this.defaultMQPushConsumer.getConsumeThreadMin() + ") "
                    + "is larger than consumeThreadMax (" + this.defaultMQPushConsumer.getConsumeThreadMax() + ")",
                null);
        }

        // consumeConcurrentlyMaxSpan
        if (this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() < 1
            || this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() > 65535) {
            throw new MQClientException(
                "consumeConcurrentlyMaxSpan Out of range [1, 65535]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // pullThresholdForQueue
        if (this.defaultMQPushConsumer.getPullThresholdForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdForQueue() > 65535) {
            throw new MQClientException(
                "pullThresholdForQueue Out of range [1, 65535]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // pullThresholdForTopic
        if (this.defaultMQPushConsumer.getPullThresholdForTopic() != -1) {
            if (this.defaultMQPushConsumer.getPullThresholdForTopic() < 1 || this.defaultMQPushConsumer.getPullThresholdForTopic() > 6553500) {
                throw new MQClientException(
                    "pullThresholdForTopic Out of range [1, 6553500]"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }
        }

        // pullThresholdSizeForQueue
        if (this.defaultMQPushConsumer.getPullThresholdSizeForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdSizeForQueue() > 1024) {
            throw new MQClientException(
                "pullThresholdSizeForQueue Out of range [1, 1024]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() != -1) {
            // pullThresholdSizeForTopic
            if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() < 1 || this.defaultMQPushConsumer.getPullThresholdSizeForTopic() > 102400) {
                throw new MQClientException(
                    "pullThresholdSizeForTopic Out of range [1, 102400]"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }
        }

        // pullInterval
        if (this.defaultMQPushConsumer.getPullInterval() < 0 || this.defaultMQPushConsumer.getPullInterval() > 65535) {
            throw new MQClientException(
                "pullInterval Out of range [0, 65535]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // consumeMessageBatchMaxSize
        if (this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() < 1
            || this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() > 1024) {
            throw new MQClientException(
                "consumeMessageBatchMaxSize Out of range [1, 1024]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // pullBatchSize
        if (this.defaultMQPushConsumer.getPullBatchSize() < 1 || this.defaultMQPushConsumer.getPullBatchSize() > 1024) {
            throw new MQClientException(
                "pullBatchSize Out of range [1, 1024]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // popInvisibleTime
        if (this.defaultMQPushConsumer.getPopInvisibleTime() < MIN_POP_INVISIBLE_TIME
            || this.defaultMQPushConsumer.getPopInvisibleTime() > MAX_POP_INVISIBLE_TIME) {
            throw new MQClientException(
                "popInvisibleTime Out of range [" + MIN_POP_INVISIBLE_TIME + ", " + MAX_POP_INVISIBLE_TIME + "]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // popBatchNums
        if (this.defaultMQPushConsumer.getPopBatchNums() <= 0 || this.defaultMQPushConsumer.getPopBatchNums() > 32) {
            throw new MQClientException(
                "popBatchNums Out of range [1, 32]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
    }

    /**
     * 遍历 topic => 订阅表达 生产 topic => 映射 构建  "%RETRY%(consumerGroup) 订阅关系
     * @throws MQClientException
     */
    private void copySubscription() throws MQClientException {
        try {
            //根据订阅表达式 构建订阅关系
            Map<String, String> sub = this.defaultMQPushConsumer.getSubscription();
            if (sub != null) {
                for (final Map.Entry<String, String> entry : sub.entrySet()) {
                    final String topic = entry.getKey();
                    final String subString = entry.getValue();
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subString);
                    this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                }
            }
            //初始化 messageListenerInner
            if (null == this.messageListenerInner) {
                this.messageListenerInner = this.defaultMQPushConsumer.getMessageListener();
            }

            switch (this.defaultMQPushConsumer.getMessageModel()) {
                case BROADCASTING:
                    break;
                case CLUSTERING:
                    //FIXME:: 重试的 topic 为  "%RETRY%consumerGroup 构建订阅构建订阅 订阅关系
                    final String retryTopic = MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup());
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(retryTopic, SubscriptionData.SUB_ALL);
                    this.rebalanceImpl.getSubscriptionInner().put(retryTopic, subscriptionData);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public MessageListener getMessageListenerInner() {
        return messageListenerInner;
    }

    /**
     * 遍历订阅关系 表 然后 根据 topic 从NameServer那边 获取路由信息
     */
    private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
        if (doNotUpdateTopicSubscribeInfoWhenSubscriptionChanged) {
            return;
        }
        //遍历订阅关系 表 然后 根据 topic 从NameServer那边 获取路由信息
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            }
        }
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return this.rebalanceImpl.getSubscriptionInner();
    }

    /**
     * 建订阅关系  发送心跳数据 和 注册消息 过滤 class
     * @param topic
     * @param subExpression
     * @throws MQClientException
     */
    public void subscribe(String topic, String subExpression) throws MQClientException {
        try {
            //构建订阅关系
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subExpression);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    /**
     * 建订阅关系 过滤类名称 过滤类  发送心跳数据 和 注册消息 过滤 class
     * @param topic
     * @param fullClassName
     * @param filterClassSource
     * @throws MQClientException
     */
    public void subscribe(String topic, String fullClassName, String filterClassSource) throws MQClientException {
        try {
            //构建订阅关系 过滤类名称 过滤类  发送心跳数据 和 注册消息 过滤 class
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, "*");
            subscriptionData.setSubString(fullClassName);
            subscriptionData.setClassFilterMode(true);
            subscriptionData.setFilterClassSource(filterClassSource);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }

        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    /**
     * 生成 topic 的订阅关系 然后发 发送心跳数据 和 注册消息 过滤 class
     * @param topic
     * @param messageSelector
     * @throws MQClientException
     */
    public void subscribe(final String topic, final MessageSelector messageSelector) throws MQClientException {
        try {
            //如果MessageSelector 过滤为 null  则订阅全部
            if (messageSelector == null) {
                subscribe(topic, SubscriptionData.SUB_ALL);
                return;
            }
            //构建订阅消息
            SubscriptionData subscriptionData = FilterAPI.build(topic,
                messageSelector.getExpression(), messageSelector.getExpressionType());
            //生成 topic 和 订阅关系 发送心跳数据 和 注册消息 过滤 class
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    /**
     * 暂停该消费者
     */
    public void suspend() {
        this.pause = true;
        log.info("suspend this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }

    /**
     * 移除该 topic 的 订阅关系
     * @param topic
     */
    public void unsubscribe(String topic) {
        this.rebalanceImpl.getSubscriptionInner().remove(topic);
    }

    public void updateConsumeOffset(MessageQueue mq, long offset) {
        this.offsetStore.updateOffset(mq, offset, false);
    }

    public void updateCorePoolSize(int corePoolSize) {
        this.consumeMessageService.updateCorePoolSize(corePoolSize);
    }

    public MessageExt viewMessage(String msgId)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }

    public RebalanceImpl getRebalanceImpl() {
        return rebalanceImpl;
    }

    public boolean isConsumeOrderly() {
        return consumeOrderly;
    }

    public void setConsumeOrderly(boolean consumeOrderly) {
        this.consumeOrderly = consumeOrderly;
    }

    public void resetOffsetByTimeStamp(long timeStamp) throws MQClientException {
        for (String topic : rebalanceImpl.getSubscriptionInner().keySet()) {
            Set<MessageQueue> mqs = rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
            if (CollectionUtils.isNotEmpty(mqs)) {
                Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>(mqs.size(), 1);
                for (MessageQueue mq : mqs) {
                    long offset = searchOffset(mq, timeStamp);
                    offsetTable.put(mq, offset);
                }
                this.mQClientFactory.resetOffset(topic, groupName(), offsetTable);
            }
        }
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    @Override
    public String groupName() {
        return this.defaultMQPushConsumer.getConsumerGroup();
    }

    @Override
    public MessageModel messageModel() {
        return this.defaultMQPushConsumer.getMessageModel();
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }

    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return this.defaultMQPushConsumer.getConsumeFromWhere();
    }

    @Override
    public Set<SubscriptionData> subscriptions() {
        return new HashSet<SubscriptionData>(this.rebalanceImpl.getSubscriptionInner().values());
    }

    @Override
    public void doRebalance() {
        if (!this.pause) {
            this.rebalanceImpl.doRebalance(this.isConsumeOrderly());
        }
    }

    /**
     * 遍历消费者 消费 队列集合 进行持久 化
     */
    @Override
    public void persistConsumerOffset() {
        try {
            this.makeSureStateOK();
            Set<MessageQueue> mqs = new HashSet<MessageQueue>();
            Set<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().keySet();
            mqs.addAll(allocateMq);

            this.offsetStore.persistAll(mqs);
        } catch (Exception e) {
            log.error("group: " + this.defaultMQPushConsumer.getConsumerGroup() + " persistConsumerOffset exception", e);
        }
    }

    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                this.rebalanceImpl.topicSubscribeInfoTable.put(topic, info);
            }
        }
    }

    @Override
    public boolean isSubscribeTopicNeedUpdate(String topic) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
            }
        }

        return false;
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultMQPushConsumer.isUnitMode();
    }

    @Override
    public ConsumerRunningInfo consumerRunningInfo() {
        ConsumerRunningInfo info = new ConsumerRunningInfo();

        Properties prop = MixAll.object2Properties(this.defaultMQPushConsumer);

        prop.put(ConsumerRunningInfo.PROP_CONSUME_ORDERLY, String.valueOf(this.consumeOrderly));
        prop.put(ConsumerRunningInfo.PROP_THREADPOOL_CORE_SIZE, String.valueOf(this.consumeMessageService.getCorePoolSize()));
        prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, String.valueOf(this.consumerStartTimestamp));

        info.setProperties(prop);

        Set<SubscriptionData> subSet = this.subscriptions();
        info.getSubscriptionSet().addAll(subSet);

        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.rebalanceImpl.getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();

            ProcessQueueInfo pqinfo = new ProcessQueueInfo();
            pqinfo.setCommitOffset(this.offsetStore.readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE));
            pq.fillProcessQueueInfo(pqinfo);
            info.getMqTable().put(mq, pqinfo);
        }

        Iterator<Entry<MessageQueue, PopProcessQueue>> popIt = this.rebalanceImpl.getPopProcessQueueTable().entrySet().iterator();
        while (popIt.hasNext()) {
            Entry<MessageQueue, PopProcessQueue> next = popIt.next();
            MessageQueue mq = next.getKey();
            PopProcessQueue pq = next.getValue();

            PopProcessQueueInfo pqinfo = new PopProcessQueueInfo();
            pq.fillPopProcessQueueInfo(pqinfo);
            info.getMqPopTable().put(mq, pqinfo);
        }

        for (SubscriptionData sd : subSet) {
            ConsumeStatus consumeStatus = this.mQClientFactory.getConsumerStatsManager().consumeStatus(this.groupName(), sd.getTopic());
            info.getStatusTable().put(sd.getTopic(), consumeStatus);
        }

        return info;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public ServiceState getServiceState() {
        return serviceState;
    }

    //Don't use this deprecated setter, which will be removed soon.
    @Deprecated
    public synchronized void setServiceState(ServiceState serviceState) {
        this.serviceState = serviceState;
    }

    public void adjustThreadPool() {
        long computeAccTotal = this.computeAccumulationTotal();
        long adjustThreadPoolNumsThreshold = this.defaultMQPushConsumer.getAdjustThreadPoolNumsThreshold();

        long incThreshold = (long) (adjustThreadPoolNumsThreshold * 1.0);

        long decThreshold = (long) (adjustThreadPoolNumsThreshold * 0.8);

        if (computeAccTotal >= incThreshold) {
            this.consumeMessageService.incCorePoolSize();
        }

        if (computeAccTotal < decThreshold) {
            this.consumeMessageService.decCorePoolSize();
        }
    }

    private long computeAccumulationTotal() {
        long msgAccTotal = 0;
        ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = this.rebalanceImpl.getProcessQueueTable();
        Iterator<Entry<MessageQueue, ProcessQueue>> it = processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue value = next.getValue();
            msgAccTotal += value.getMsgAccCnt();
        }

        return msgAccTotal;
    }

    public List<QueueTimeSpan> queryConsumeTimeSpan(final String topic)
        throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        List<QueueTimeSpan> queueTimeSpan = new ArrayList<QueueTimeSpan>();
        TopicRouteData routeData = this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, 3000);
        for (BrokerData brokerData : routeData.getBrokerDatas()) {
            String addr = brokerData.selectBrokerAddr();
            queueTimeSpan.addAll(this.mQClientFactory.getMQClientAPIImpl().queryConsumeTimeSpan(addr, topic, groupName(), 3000));
        }

        return queueTimeSpan;
    }

    public void tryResetPopRetryTopic(final List<MessageExt> msgs, String consumerGroup) {
        //如果消息 topic 是以 "%RETRY%consumerGroup_" 为开头 则去掉这个 开头
        String popRetryPrefix = MixAll.RETRY_GROUP_TOPIC_PREFIX + consumerGroup + "_";
        for (MessageExt msg : msgs) {
            if (msg.getTopic().startsWith(popRetryPrefix)) {
                String normalTopic = KeyBuilder.parseNormalTopic(msg.getTopic(), consumerGroup);
                if (normalTopic != null && !normalTopic.isEmpty()) {
                    msg.setTopic(normalTopic);
                }
            }
        }
    }


    public void resetRetryAndNamespace(final List<MessageExt> msgs, String consumerGroup) {
        //获取 消费组的 topic
        final String groupTopic = MixAll.getRetryTopic(consumerGroup);
        //遍历 消息 如果 该消息的 topic 为该 消费组的 topic  获取该 消息 重试 topic 设置
        for (MessageExt msg : msgs) {
            String retryTopic = msg.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
            if (retryTopic != null && groupTopic.equals(msg.getTopic())) {
                msg.setTopic(retryTopic);
            }
            //去掉对应 namespace
            if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
            }
        }
    }

    public ConsumeMessageService getConsumeMessageService() {
        return consumeMessageService;
    }

    public void setConsumeMessageService(ConsumeMessageService consumeMessageService) {
        this.consumeMessageService = consumeMessageService;

    }

    public void setPullTimeDelayMillsWhenException(long pullTimeDelayMillsWhenException) {
        this.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
    }

    int[] getPopDelayLevel() {
        return popDelayLevel;
    }
}
