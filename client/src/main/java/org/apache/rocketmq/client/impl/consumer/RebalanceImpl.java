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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueAssignment;
import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;

public abstract class RebalanceImpl {
    protected static final InternalLogger log = ClientLogger.getLog();
    /**
     * key 为 MessageQueue, value 为  ProcessQueue
     */
    protected final ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = new ConcurrentHashMap<MessageQueue, ProcessQueue>(64);
    /**
     * key 为 MessageQueue, value 为  ProcessQueue
     */
    protected final ConcurrentMap<MessageQueue, PopProcessQueue> popProcessQueueTable = new ConcurrentHashMap<MessageQueue, PopProcessQueue>(64);
    /**
     * key 为  topic , value 为   MessageQueue  topic 的 消息 队列
     */
    protected final ConcurrentMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable =
        new ConcurrentHashMap<String, Set<MessageQueue>>();
    /**
     * key 为 topic ,value 为 SubscriptionData  订阅关系
     */
    protected final ConcurrentMap<String /* topic */, SubscriptionData> subscriptionInner =
        new ConcurrentHashMap<String, SubscriptionData>();
    /**
     * 消费者组名
     */
    protected String consumerGroup;
    /**
     * 消息模式
     */
    protected MessageModel messageModel;
    /**
     * 队列客户端的分配策略
     */
    protected AllocateMessageQueueStrategy allocateMessageQueueStrategy;
    /**
     *客户端
     */
    protected MQClientInstance mQClientFactory;
    /**
     * 检查次数
     */
    private static final int TIMEOUT_CHECK_TIMES = 3;
    private static final int QUERY_ASSIGNMENT_TIMEOUT = 3000;
    /**
     * key 为 topic ,value 为 topic
     */
    private Map<String, String> topicBrokerRebalance = new ConcurrentHashMap<String, String>();
    /**
     * key 为 topic ,value 为 topic
     */
    private Map<String, String> topicClientRebalance = new ConcurrentHashMap<String, String>();

    public RebalanceImpl(String consumerGroup, MessageModel messageModel,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MQClientInstance mQClientFactory) {
        this.consumerGroup = consumerGroup;
        this.messageModel = messageModel;
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.mQClientFactory = mQClientFactory;
    }

    /**
     * 将 MessageQueue 批量解锁
     * @param mq
     * @param oneway
     */
    public void unlock(final MessageQueue mq, final boolean oneway) {
        //根据 MessageQueue.BrokerName 获取 该 broker 节点 的主 broker
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(this.mQClientFactory.getBrokerNameFromMessageQueue(mq), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                //将 MessageQueue 批量解锁
                this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);
                log.warn("unlock messageQueue. group:{}, clientId:{}, mq:{}",
                    this.consumerGroup,
                    this.mQClientFactory.getClientId(),
                    mq);
            } catch (Exception e) {
                log.error("unlockBatchMQ exception, " + mq, e);
            }
        }
    }

    public void unlockAll(final boolean oneway) {
        //将 processQueueTable 根据 brokerName  进行 消息队列的 划分
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        for (final Map.Entry<String, Set<MessageQueue>> entry : brokerMqs.entrySet()) {
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty()) {
                continue;
            }
            //找 到 该 broker 的 节点 主 broker  brokerAddress
            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                UnlockBatchRequestBody requestBody = new UnlockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);
                //将 该  broker 的 MessageQueue 进行 批量解锁
                try {
                    this.mQClientFactory.getMQClientAPIImpl().unlockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000, oneway);
                    //将 批量就锁 MessageQueue 的 ProcessQueue 标记 为 未上锁 FIXME 万一 没有解锁成功呢 ?
                    for (MessageQueue mq : mqs) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            processQueue.setLocked(false);
                            log.info("the message queue unlock OK, Group: {} {}", this.consumerGroup, mq);
                        }
                    }
                } catch (Exception e) {
                    log.error("unlockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    /**
     * 将 processQueueTable 根据 brokerName  进行 消息队列的 划分
     * @return
     */
    private HashMap<String/* brokerName */, Set<MessageQueue>> buildProcessQueueTableByBrokerName() {
        HashMap<String, Set<MessageQueue>> result = new HashMap<String, Set<MessageQueue>>();
        //遍历 processQueueTable 按 brokerName 将 MessageQueue 进行 划分
        for (Map.Entry<MessageQueue, ProcessQueue> entry : this.processQueueTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            ProcessQueue pq = entry.getValue();

            if (pq.isDropped()) {
                continue;
            }
            //获取队列的 brokerName
            String destBrokerName = this.mQClientFactory.getBrokerNameFromMessageQueue(mq);
            //获取该 BrokerName 获取 MessageQueue 集合 不存在 对应 映射 则进行创建 将 消息队列 添加到对应 消息队列集合当中
            Set<MessageQueue> mqs = result.get(destBrokerName);
            if (null == mqs) {
                mqs = new HashSet<MessageQueue>();
                result.put(mq.getBrokerName(), mqs);
            }

            mqs.add(mq);
        }

        return result;
    }

    /**
     * 将 MessageQueue 批量上锁 返回成功 上锁的 MessageQueue 将 成功上锁的 ProcessQueue 标记上锁 修上锁 时间
     * @param mq
     * @return
     */
    public boolean lock(final MessageQueue mq) {
        //根据 MessageQueue.BrokerName 获取 该 broker 节点 的主 broker
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(this.mQClientFactory.getBrokerNameFromMessageQueue(mq), MixAll.MASTER_ID, true);
        if (findBrokerResult != null) {
            LockBatchRequestBody requestBody = new LockBatchRequestBody();
            requestBody.setConsumerGroup(this.consumerGroup);
            requestBody.setClientId(this.mQClientFactory.getClientId());
            requestBody.getMqSet().add(mq);

            try {
                //将 MessageQueue 批量上锁 返回成功 上锁的 MessageQueue
                Set<MessageQueue> lockedMq =
                    this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
                //遍历 成功 上锁的 MessageQueue 将 成功上锁的 ProcessQueue 标记上锁 修上锁 时间
                for (MessageQueue mmqq : lockedMq) {
                    ProcessQueue processQueue = this.processQueueTable.get(mmqq);
                    if (processQueue != null) {
                        processQueue.setLocked(true);
                        processQueue.setLastLockTimestamp(System.currentTimeMillis());
                    }
                }
                //如果 成功 上锁的 MessageQueue 包含 该 MessageQueue 说明上锁 成功
                boolean lockOK = lockedMq.contains(mq);
                log.info("message queue lock {}, {} {}", lockOK ? "OK" : "Failed", this.consumerGroup, mq);
                return lockOK;
            } catch (Exception e) {
                log.error("lockBatchMQ exception, " + mq, e);
            }
        }

        return false;
    }

    /**
     * 遍历 processQueueTable 将 里面 的 MessageQueue 全部 上锁
     */
    public void lockAll() {
        //将 processQueueTable 根据 brokerName  进行 消息队列的 划分
        HashMap<String, Set<MessageQueue>> brokerMqs = this.buildProcessQueueTableByBrokerName();

        Iterator<Entry<String, Set<MessageQueue>>> it = brokerMqs.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Set<MessageQueue>> entry = it.next();
            final String brokerName = entry.getKey();
            final Set<MessageQueue> mqs = entry.getValue();

            if (mqs.isEmpty()) {
                continue;
            }
            //找 到 该 broker 的 节点 主 broker  brokerAddress
            FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInSubscribe(brokerName, MixAll.MASTER_ID, true);
            if (findBrokerResult != null) {
                //将 该  broker 的 MessageQueue 进行 批量上锁
                LockBatchRequestBody requestBody = new LockBatchRequestBody();
                requestBody.setConsumerGroup(this.consumerGroup);
                requestBody.setClientId(this.mQClientFactory.getClientId());
                requestBody.setMqSet(mqs);

                try {
                    Set<MessageQueue> lockOKMQSet =
                        this.mQClientFactory.getMQClientAPIImpl().lockBatchMQ(findBrokerResult.getBrokerAddr(), requestBody, 1000);
                    //遍历 成功 上锁的 MessageQueue 将 成功上锁的 ProcessQueue 标记上锁 修上锁 时间
                    for (MessageQueue mq : lockOKMQSet) {
                        ProcessQueue processQueue = this.processQueueTable.get(mq);
                        if (processQueue != null) {
                            if (!processQueue.isLocked()) {
                                log.info("the message queue locked OK, Group: {} {}", this.consumerGroup, mq);
                            }

                            processQueue.setLocked(true);
                            processQueue.setLastLockTimestamp(System.currentTimeMillis());
                        }
                    }
                    //遍历 需要上锁 MessageQueue 若没成功 上锁 则将 processQueue 上锁标记 改成 false
                    for (MessageQueue mq : mqs) {
                        if (!lockOKMQSet.contains(mq)) {
                            ProcessQueue processQueue = this.processQueueTable.get(mq);
                            if (processQueue != null) {
                                processQueue.setLocked(false);
                                log.warn("the message queue locked Failed, Group: {} {}", this.consumerGroup, mq);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error("lockBatchMQ exception, " + mqs, e);
                }
            }
        }
    }

    /**
     * 是否是客户端做负载平衡
     * @param topic
     * @return
     */
    public boolean clientRebalance(String topic) {
        return true;
    }

    public boolean doRebalance(final boolean isOrder) {
        boolean balanced = true;
        //遍历订阅消息
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                try {
                    //不是客户端做负载均衡
                    if (!clientRebalance(topic) && tryQueryAssignment(topic)) {
                        balanced = this.getRebalanceResultFromBroker(topic, isOrder);
                    } else {
                        //客户端做负载均衡
                        balanced = this.rebalanceByTopic(topic, isOrder);
                    }
                } catch (Throwable e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("rebalance Exception", e);
                        balanced = false;
                    }
                }
            }
        }

        this.truncateMessageQueueNotMyTopic();

        return balanced;
    }

    private boolean tryQueryAssignment(String topic) {
        if (topicClientRebalance.containsKey(topic)) {
            return false;
        }
        if (topicBrokerRebalance.containsKey(topic)) {
            return true;
        }
        //分配策略的名称
        String strategyName = allocateMessageQueueStrategy != null ? allocateMessageQueueStrategy.getName() : null;
        int retryTimes = 0;
        //重试 查询 topic 队列分配
        while (retryTimes++ < TIMEOUT_CHECK_TIMES) {
            try {
                Set<MessageQueueAssignment> resultSet = mQClientFactory.queryAssignment(topic, consumerGroup,
                    strategyName, messageModel, QUERY_ASSIGNMENT_TIMEOUT / TIMEOUT_CHECK_TIMES * retryTimes);
                topicBrokerRebalance.put(topic, topic);
                return true;
            } catch (Throwable t) {
                if (!(t instanceof RemotingTimeoutException)) {
                    log.error("tryQueryAssignment error.", t);
                    topicClientRebalance.put(topic, topic);
                    return false;
                }
            }
        }
        if (retryTimes >= TIMEOUT_CHECK_TIMES) {
            //如果从为成功 或者 超过 次 强制 rebalance
            // if never success before and timeout exceed TIMEOUT_CHECK_TIMES, force client rebalance
            topicClientRebalance.put(topic, topic);
            return false;
        }
        return true;
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return subscriptionInner;
    }

    private boolean rebalanceByTopic(final String topic, final boolean isOrder) {
        boolean balanced = true;
        switch (messageModel) {
            //广播模式 则 获取 该 topic 的 MessageQueue
            case BROADCASTING: {
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                if (mqSet != null) {
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, mqSet, isOrder);
                    if (changed) {
                        //获取队列监听器 通知 消息队列 发生改变
                        this.messageQueueChanged(topic, mqSet, mqSet);
                        log.info("messageQueueChanged {} {} {} {}", consumerGroup, topic, mqSet, mqSet);
                    }

                    balanced = mqSet.equals(getWorkingMessageQueue(topic));
                } else {
                    //获取队列监听器 通知 消息队列 发生改变
                    this.messageQueueChanged(topic, Collections.<MessageQueue>emptySet(), Collections.<MessageQueue>emptySet());
                    log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                }
                break;
            }
            case CLUSTERING: {
                //集群模式 则 获取 该 topic 的 MessageQueue
                Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
                //根据该 topic 该消费组 获取 该消费组的 客户端 id 集合
                List<String> cidAll = this.mQClientFactory.findConsumerIdList(topic, consumerGroup);
                if (null == mqSet) {
                    //若该 topic 不是 重试 topic 并且 该 topic 的 MessageQueue 为 空
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        //TODO:: messageQueueChanged 由子类 实现 获取队列监听器 通知 消息队列 发生改变
                        this.messageQueueChanged(topic, Collections.<MessageQueue>emptySet(), Collections.<MessageQueue>emptySet());
                        log.warn("doRebalance, {}, but the topic[{}] not exist.", consumerGroup, topic);
                    }
                }

                if (null == cidAll) {
                    log.warn("doRebalance, {} {}, get consumer id list failed", consumerGroup, topic);
                }

                if (mqSet != null && cidAll != null) {
                    List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
                    mqAll.addAll(mqSet);
                    // 该 topic 的 MessageQueue 进行 排序  该消费组的 客户端id 进行排序
                    Collections.sort(mqAll);
                    Collections.sort(cidAll);

                    AllocateMessageQueueStrategy strategy = this.allocateMessageQueueStrategy;

                    //根据分配策略进行 获取该客户端 分配到的 MessageQueue
                    List<MessageQueue> allocateResult = null;
                    try {
                        allocateResult = strategy.allocate(
                            this.consumerGroup,
                            this.mQClientFactory.getClientId(),
                            mqAll,
                            cidAll);
                    } catch (Throwable e) {
                        log.error("allocate message queue exception. strategy name: {}, ex: {}", strategy.getName(), e);
                        return false;
                    }

                    Set<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
                    if (allocateResult != null) {
                        allocateResultSet.addAll(allocateResult);
                    }
                    //更新 processQueueTable
                    boolean changed = this.updateProcessQueueTableInRebalance(topic, allocateResultSet, isOrder);
                    if (changed) {
                        log.info(
                            "client rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, mqAllSize={}, cidAllSize={}, rebalanceResultSize={}, rebalanceResultSet={}",
                            strategy.getName(), consumerGroup, topic, this.mQClientFactory.getClientId(), mqSet.size(), cidAll.size(),
                            allocateResultSet.size(), allocateResultSet);
                        //TODO:: messageQueueChanged 由子类实现 获取队列监听器 通知 消息队列 发生改变
                        this.messageQueueChanged(topic, mqSet, allocateResultSet);
                    }
                    //判断 topic 的 MessageQueue 是否 相同
                    balanced = allocateResultSet.equals(getWorkingMessageQueue(topic));
                }
                break;
            }
            default:
                break;
        }

        return balanced;
    }

    private boolean getRebalanceResultFromBroker(final String topic, final boolean isOrder) {
        //获取该分配策略的名称
        String strategyName = this.allocateMessageQueueStrategy.getName();
        Set<MessageQueueAssignment> messageQueueAssignments;
        try {
            //获取 该 topic 消费组的 消息队列的分配 策略
            messageQueueAssignments = this.mQClientFactory.queryAssignment(topic, consumerGroup,
                strategyName, messageModel, QUERY_ASSIGNMENT_TIMEOUT);
        } catch (Exception e) {
            log.error("allocate message queue exception. strategy name: {}, ex: {}", strategyName, e);
            return false;
        }

        // null means invalid result, we should skip the update logic
        if (messageQueueAssignments == null) {
            return false;
        }
        //获取消息队列分配 的 消息的队列
        Set<MessageQueue> mqSet = new HashSet<MessageQueue>();
        for (MessageQueueAssignment messageQueueAssignment : messageQueueAssignments) {
            if (messageQueueAssignment.getMessageQueue() != null) {
                mqSet.add(messageQueueAssignment.getMessageQueue());
            }
        }
        Set<MessageQueue> mqAll = null;
        //更新分配信息
        boolean changed = this.updateMessageQueueAssignment(topic, messageQueueAssignments, isOrder);
        if (changed) {
            log.info("broker rebalanced result changed. allocateMessageQueueStrategyName={}, group={}, topic={}, clientId={}, assignmentSet={}",
                strategyName, consumerGroup, topic, this.mQClientFactory.getClientId(), messageQueueAssignments);
            //TODO:: messageQueueChanged 由子类实现 获取队列监听器 通知 消息队列 发生改变
            this.messageQueueChanged(topic, mqAll, mqSet);
        }
        //判断 topic 的 MessageQueue 是否 相同
        return mqSet.equals(getWorkingMessageQueue(topic));
    }

    /**
     * 查找该 topic 的 MessageQueue 并且 该 MessageQueue 的 ProcessQueue 没有 丢弃
     * @param topic
     * @return
     */
    private Set<MessageQueue> getWorkingMessageQueue(String topic) {
        Set<MessageQueue> queueSet = new HashSet<MessageQueue>();
        //遍历 processQueueTable 查找 该 topic MessageQueue 并且 该 MessageQueue 的 ProcessQueue 没有 丢弃
        for (Entry<MessageQueue, ProcessQueue> entry : this.processQueueTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            ProcessQueue pq = entry.getValue();

            if (mq.getTopic().equals(topic) && !pq.isDropped()) {
                queueSet.add(mq);
            }
        }
        //遍历 popProcessQueueTable 查找 该 topic MessageQueue 并且 该 MessageQueue 的 PopProcessQueue 没有 丢弃
        for (Entry<MessageQueue, PopProcessQueue> entry : this.popProcessQueueTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            PopProcessQueue pq = entry.getValue();

            if (mq.getTopic().equals(topic) && !pq.isDropped()) {
                queueSet.add(mq);
            }
        }

        return queueSet;
    }

    private void truncateMessageQueueNotMyTopic() {
        //获取订阅关系
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        //遍历 processQueueTable 不存在 订阅关系 则从 processQueueTable 移除 将 MessageQueue 对应的  ProcessQueue 标记 丢弃
        for (MessageQueue mq : this.processQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {

                ProcessQueue pq = this.processQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }

        //遍历 popProcessQueueTable 不存在 订阅关系 则从 processQueueTable 移除 将 MessageQueue 对应的  ProcessQueue 标记 丢弃
        for (MessageQueue mq : this.popProcessQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {

                PopProcessQueue pq = this.popProcessQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDropped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary pop mq, {}", consumerGroup, mq);
                }
            }
        }
        //topicBrokerRebalance 移除不存在的topic
        Iterator<Map.Entry<String, String>> clientIter = topicClientRebalance.entrySet().iterator();
        while (clientIter.hasNext()) {
            if (!subTable.containsKey(clientIter.next().getKey())) {
                clientIter.remove();
            }
        }
        //topicBrokerRebalance 移除不存在的topic
        Iterator<Map.Entry<String, String>> brokerIter = topicBrokerRebalance.entrySet().iterator();
        while (brokerIter.hasNext()) {
            if (!subTable.containsKey(brokerIter.next().getKey())) {
                brokerIter.remove();
            }
        }
    }

    private boolean updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet,
        final boolean isOrder) {
        boolean changed = false;

        // drop process queues no longer belong me
        HashMap<MessageQueue, ProcessQueue> removeQueueMap = new HashMap<MessageQueue, ProcessQueue>(this.processQueueTable.size());
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        //遍历 processQueueTable 查找 该 topic 下 MessageQueue
        // MessageQueue 不在 mqSet 则 将 ProcessQueue 标记 dropped
        // ProcessQueue 长时间没有 被 获取消息 并且 该 消费类型 是 CONSUME_PASSIVELY("PUSH") 则 将 ProcessQueue 标记 dropped
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();

            if (mq.getTopic().equals(topic)) {
                if (!mqSet.contains(mq)) {
                    pq.setDropped(true);
                    removeQueueMap.put(mq, pq);
                } else if (pq.isPullExpired() && this.consumeType() == ConsumeType.CONSUME_PASSIVELY) {
                    pq.setDropped(true);
                    removeQueueMap.put(mq, pq);
                    log.error("[BUG]doRebalance, {}, try remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                        consumerGroup, mq);
                }
            }
        }

        // remove message queues no longer belong me
        //移除 不必要的 消息队列 ProcessQueue TODO:: 由子类 决定
        for (Entry<MessageQueue, ProcessQueue> entry : removeQueueMap.entrySet()) {
            MessageQueue mq = entry.getKey();
            ProcessQueue pq = entry.getValue();

            if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                this.processQueueTable.remove(mq);
                changed = true;
                log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
            }
        }
        //对 processQueueTable 进行 更新
        // add new message queue
        boolean allMQLocked = true;
        List<PullRequest> pullRequestList = new ArrayList<PullRequest>();
        //遍历 mqSet
        for (MessageQueue mq : mqSet) {
            //若果 processQueueTable 不包含 该 MessageQueue 则进行添加
            if (!this.processQueueTable.containsKey(mq)) {
                //如果有序 则将该 mq 进行上锁 上锁失败 则 allMQLocked标记 false
                if (isOrder && !this.lock(mq)) {
                    log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                    allMQLocked = false;
                    continue;
                }
                //TODO:: removeDirtyOffset 由子类实现  createProcessQueue 由子类实现
                this.removeDirtyOffset(mq);
                ProcessQueue pq = createProcessQueue(topic);
                //现将 ProcessQueue 标记为 上锁
                pq.setLocked(true);
                //TODO:: computePullFromWhereWithException 由子类实现 计算 下一个偏移量
                long nextOffset = this.computePullFromWhere(mq);
                //如果 nextOffset > 0 MessageQueue => ProcessQueue 映射 生成 PullRequest 标记 changed
                if (nextOffset >= 0) {
                    //MessageQueue => ProcessQueue 映射
                    ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                    } else {
                        log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                        PullRequest pullRequest = new PullRequest();
                        pullRequest.setConsumerGroup(consumerGroup);
                        pullRequest.setNextOffset(nextOffset);
                        pullRequest.setMessageQueue(mq);
                        pullRequest.setProcessQueue(pq);
                        pullRequestList.add(pullRequest);
                        changed = true;
                    }
                } else {
                    log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                }
            }

        }
        //若果没有全部上锁 则延迟 balance 延迟唤醒
        if (!allMQLocked) {
            mQClientFactory.rebalanceLater(500);
        }
        //TODO :: dispatchPullRequest 由子类实现
        this.dispatchPullRequest(pullRequestList, 500);

        return changed;
    }

    private boolean updateMessageQueueAssignment(final String topic, final Set<MessageQueueAssignment> assignments,
        final boolean isOrder) {
        boolean changed = false;

        Map<MessageQueue, MessageQueueAssignment> mq2PushAssignment = new HashMap<MessageQueue, MessageQueueAssignment>();
        Map<MessageQueue, MessageQueueAssignment> mq2PopAssignment = new HashMap<MessageQueue, MessageQueueAssignment>();
        //进行 MessageRequestMode 分类
        //根据获取消息 方式 进行分组 POP mq2PopAssignment; PULL mq2PushAssignment
        for (MessageQueueAssignment assignment : assignments) {
            MessageQueue messageQueue = assignment.getMessageQueue();
            if (messageQueue == null) {
                continue;
            }
            if (MessageRequestMode.POP == assignment.getMode()) {
                mq2PopAssignment.put(messageQueue, assignment);
            } else {
                mq2PushAssignment.put(messageQueue, assignment);
            }
        }
        //topic 不是重试 topic
        //FIXME:: 如果 有 push 类型 , 没有 pop 类型的 则 生成 该 consumerGroup 的 重试 topic 的订阅
        // 如果只有 pop 类型 , 没有 push 类型 类型的  则 取消 该 consumerGroup 的 重试 topic 的订阅
        if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            if (mq2PopAssignment.isEmpty() && !mq2PushAssignment.isEmpty()) {
                //pop switch to push
                //subscribe pop retry topic
                try {
                    final String retryTopic = KeyBuilder.buildPopRetryTopic(topic, getConsumerGroup());
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(retryTopic, SubscriptionData.SUB_ALL);
                    getSubscriptionInner().put(retryTopic, subscriptionData);
                } catch (Exception ignored) {
                }

            } else if (!mq2PopAssignment.isEmpty() && mq2PushAssignment.isEmpty()) {
                //push switch to pop
                //unsubscribe pop retry topic
                try {
                    final String retryTopic = KeyBuilder.buildPopRetryTopic(topic, getConsumerGroup());
                    getSubscriptionInner().remove(retryTopic);
                } catch (Exception ignored) {
                }

            }
        }
        //对 processQueueTable 进行 移除
        {
            // drop process queues no longer belong me
            HashMap<MessageQueue, ProcessQueue> removeQueueMap = new HashMap<MessageQueue, ProcessQueue>(this.processQueueTable.size());
            Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
            //遍历 processQueueTable 查找 该 topic 下 MessageQueue
            // MessageQueue 不再是 push 则 将 ProcessQueue 标记 dropped
            // ProcessQueue 长时间没有 被 获取消息 并且 该 消费类型 是 CONSUME_PASSIVELY("PUSH") 则 将 ProcessQueue 标记 dropped
            while (it.hasNext()) {
                Entry<MessageQueue, ProcessQueue> next = it.next();
                MessageQueue mq = next.getKey();
                ProcessQueue pq = next.getValue();

                if (mq.getTopic().equals(topic)) {
                    if (!mq2PushAssignment.containsKey(mq)) {
                        pq.setDropped(true);
                        removeQueueMap.put(mq, pq);
                    } else if (pq.isPullExpired() && this.consumeType() == ConsumeType.CONSUME_PASSIVELY) {
                        pq.setDropped(true);
                        removeQueueMap.put(mq, pq);
                        log.error("[BUG]doRebalance, {}, try remove unnecessary mq, {}, because pull is pause, so try to fixed it",
                            consumerGroup, mq);
                    }
                }
            }
            // remove message queues no longer belong me
            //移除 不必要的 消息队列 ProcessQueue TODO:: 由子类 决定
            for (Entry<MessageQueue, ProcessQueue> entry : removeQueueMap.entrySet()) {
                MessageQueue mq = entry.getKey();
                ProcessQueue pq = entry.getValue();

                if (this.removeUnnecessaryMessageQueue(mq, pq)) {
                    this.processQueueTable.remove(mq);
                    changed = true;
                    log.info("doRebalance, {}, remove unnecessary mq, {}", consumerGroup, mq);
                }
            }
        }
        //对 popProcessQueueTable 进行 移除
        {
            HashMap<MessageQueue, PopProcessQueue> removeQueueMap = new HashMap<MessageQueue, PopProcessQueue>(this.popProcessQueueTable.size());
            Iterator<Entry<MessageQueue, PopProcessQueue>> it = this.popProcessQueueTable.entrySet().iterator();
            //遍历 popProcessQueueTable 查找 该 topic 下 MessageQueue
            // MessageQueue 不再是 pop 则 将 ProcessQueue 标记 dropped
            // ProcessQueue 长时间没有 被 获取消息 并且 该 消费类型 是 CONSUME_PASSIVELY("PUSH") 则 将 ProcessQueue 标记 dropped
            while (it.hasNext()) {
                Entry<MessageQueue, PopProcessQueue> next = it.next();
                MessageQueue mq = next.getKey();
                PopProcessQueue pq = next.getValue();

                if (mq.getTopic().equals(topic)) {
                    if (!mq2PopAssignment.containsKey(mq)) {
                        //the queue is no longer your assignment
                        pq.setDropped(true);
                        removeQueueMap.put(mq, pq);
                    } else if (pq.isPullExpired() && this.consumeType() == ConsumeType.CONSUME_PASSIVELY) {
                        pq.setDropped(true);
                        removeQueueMap.put(mq, pq);
                        log.error("[BUG]doRebalance, {}, try remove unnecessary pop mq, {}, because pop is pause, so try to fixed it",
                            consumerGroup, mq);
                    }
                }
            }
            // remove message queues no longer belong me
            //移除 不必要的 消息队列 ProcessQueue TODO:: 由子类 决定
            for (Entry<MessageQueue, PopProcessQueue> entry : removeQueueMap.entrySet()) {
                MessageQueue mq = entry.getKey();
                PopProcessQueue pq = entry.getValue();

                if (this.removeUnnecessaryPopMessageQueue(mq, pq)) {
                    this.popProcessQueueTable.remove(mq);
                    changed = true;
                    log.info("doRebalance, {}, remove unnecessary pop mq, {}", consumerGroup, mq);
                }
            }
        }

        //对 processQueueTable 进行 更新
        {
            // add new message queue
            boolean allMQLocked = true;
            List<PullRequest> pullRequestList = new ArrayList<PullRequest>();
            //遍历 PushAssignment
            for (MessageQueue mq : mq2PushAssignment.keySet()) {
                //若果 processQueueTable 不包含 该 MessageQueue 则进行添加
                if (!this.processQueueTable.containsKey(mq)) {
                    //如果有序 则将该 mq 进行上锁 上锁失败 则 allMQLocked标记 false
                    if (isOrder && !this.lock(mq)) {
                        log.warn("doRebalance, {}, add a new mq failed, {}, because lock failed", consumerGroup, mq);
                        allMQLocked = false;
                        continue;
                    }
                    //TODO:: removeDirtyOffset 由子类实现  createProcessQueue 由子类实现
                    this.removeDirtyOffset(mq);
                    ProcessQueue pq = createProcessQueue();
                    //现将 ProcessQueue 标记为 上锁
                    pq.setLocked(true);
                    long nextOffset = -1L;
                    try {
                        //TODO:: computePullFromWhereWithException 由子类实现 计算 下一个偏移量
                        nextOffset = this.computePullFromWhereWithException(mq);
                    } catch (Exception e) {
                        log.info("doRebalance, {}, compute offset failed, {}", consumerGroup, mq);
                        continue;
                    }
                    //如果 nextOffset > 0 MessageQueue => ProcessQueue 映射 生成 PullRequest 标记 changed
                    if (nextOffset >= 0) {
                        //MessageQueue => ProcessQueue 映射
                        ProcessQueue pre = this.processQueueTable.putIfAbsent(mq, pq);
                        if (pre != null) {
                            log.info("doRebalance, {}, mq already exists, {}", consumerGroup, mq);
                        } else {
                            log.info("doRebalance, {}, add a new mq, {}", consumerGroup, mq);
                            PullRequest pullRequest = new PullRequest();
                            pullRequest.setConsumerGroup(consumerGroup);
                            pullRequest.setNextOffset(nextOffset);
                            pullRequest.setMessageQueue(mq);
                            pullRequest.setProcessQueue(pq);
                            pullRequestList.add(pullRequest);
                            changed = true;
                        }
                    } else {
                        log.warn("doRebalance, {}, add new mq failed, {}", consumerGroup, mq);
                    }
                }
            }
            //若果没有全部上锁 则延迟 balance 延迟唤醒
            if (!allMQLocked) {
                mQClientFactory.rebalanceLater(500);
            }
            //TODO :: dispatchPullRequest 由子类实现
            this.dispatchPullRequest(pullRequestList, 500);
        }
        //对 popProcessQueueTable 进行 更新
        {
            // add new message queue
            List<PopRequest> popRequestList = new ArrayList<PopRequest>();
            //遍历 PopAssignment
            for (MessageQueue mq : mq2PopAssignment.keySet()) {
                if (!this.popProcessQueueTable.containsKey(mq)) {
                    //TODO:: createProcessQueue 由子类实现
                    PopProcessQueue pq = createPopProcessQueue();
                    //如果  MessageQueue => ProcessQueue 映射 生成 popRequest 标记 changed
                    PopProcessQueue pre = this.popProcessQueueTable.putIfAbsent(mq, pq);
                    if (pre != null) {
                        log.info("doRebalance, {}, mq pop already exists, {}", consumerGroup, mq);
                    } else {
                        log.info("doRebalance, {}, add a new pop mq, {}", consumerGroup, mq);
                        PopRequest popRequest = new PopRequest();
                        popRequest.setTopic(topic);
                        popRequest.setConsumerGroup(consumerGroup);
                        popRequest.setMessageQueue(mq);
                        popRequest.setPopProcessQueue(pq);
                        popRequest.setInitMode(getConsumeInitMode());
                        popRequestList.add(popRequest);
                        changed = true;
                    }
                }
            }
            //TODO :: dispatchPopPullRequest 由子类实现
            this.dispatchPopPullRequest(popRequestList, 500);
        }

        return changed;
    }

    /**
     * 获取队列监听器 通知 消息队列 发生改变
     * @param topic
     * @param mqAll
     * @param mqDivided
     */
    public abstract void messageQueueChanged(final String topic, final Set<MessageQueue> mqAll,
        final Set<MessageQueue> mqDivided);

    public abstract boolean removeUnnecessaryMessageQueue(final MessageQueue mq, final ProcessQueue pq);

    public boolean removeUnnecessaryPopMessageQueue(final MessageQueue mq, final PopProcessQueue pq) {
        return true;
    }

    public abstract ConsumeType consumeType();

    public abstract void removeDirtyOffset(final MessageQueue mq);

    /**
     * When the network is unstable, using this interface may return wrong offset.
     * It is recommended to use computePullFromWhereWithException instead.
     * @param mq
     * @return offset
     */
    @Deprecated
    public abstract long computePullFromWhere(final MessageQueue mq);

    public abstract long computePullFromWhereWithException(final MessageQueue mq) throws MQClientException;

    public abstract int getConsumeInitMode();

    /**
     * 获取消息
     * @param pullRequestList
     * @param delay
     */
    public abstract void dispatchPullRequest(final List<PullRequest> pullRequestList, final long delay);
    /**
     * 获取消息
     * @param pullRequestList
     * @param delay
     */
    public abstract void dispatchPopPullRequest(final List<PopRequest> pullRequestList, final long delay);

    public abstract ProcessQueue createProcessQueue();

    public abstract PopProcessQueue createPopProcessQueue();

    public abstract ProcessQueue createProcessQueue(String topicName);

    public void removeProcessQueue(final MessageQueue mq) {
        ProcessQueue prev = this.processQueueTable.remove(mq);
        if (prev != null) {
            boolean droped = prev.isDropped();
            prev.setDropped(true);
            this.removeUnnecessaryMessageQueue(mq, prev);
            log.info("Fix Offset, {}, remove unnecessary mq, {} Droped: {}", consumerGroup, mq, droped);
        }
    }

    public ConcurrentMap<MessageQueue, ProcessQueue> getProcessQueueTable() {
        return processQueueTable;
    }

    public ConcurrentMap<MessageQueue, PopProcessQueue> getPopProcessQueueTable() {
        return popProcessQueueTable;
    }

    public ConcurrentMap<String, Set<MessageQueue>> getTopicSubscribeInfoTable() {
        return topicSubscribeInfoTable;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public AllocateMessageQueueStrategy getAllocateMessageQueueStrategy() {
        return allocateMessageQueueStrategy;
    }

    public void setAllocateMessageQueueStrategy(AllocateMessageQueueStrategy allocateMessageQueueStrategy) {
        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    /**
     * 将  processQueueTable 和 popProcessQueueTable  的 ProcessQueue 标记丢弃 然后清空这两张表
     */
    public void destroy() {
        //遍历 processQueueTable 当中  ProcessQueue 将其 丢弃 然后清空 processQueueTable
        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            next.getValue().setDropped(true);
        }

        this.processQueueTable.clear();
        //遍历 popProcessQueueTable 当中  PopProcessQueue 将其 丢弃 然后清空 PopProcessQueue
        Iterator<Entry<MessageQueue, PopProcessQueue>> popIt = this.popProcessQueueTable.entrySet().iterator();
        while (popIt.hasNext()) {
            Entry<MessageQueue, PopProcessQueue> next = popIt.next();
            next.getValue().setDropped(true);
        }
        this.popProcessQueueTable.clear();
    }

}
