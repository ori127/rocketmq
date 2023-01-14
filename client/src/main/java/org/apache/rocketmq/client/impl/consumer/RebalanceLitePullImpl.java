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

import java.util.List;
import java.util.Set;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

public class RebalanceLitePullImpl extends RebalanceImpl {

    private final DefaultLitePullConsumerImpl litePullConsumerImpl;

    public RebalanceLitePullImpl(DefaultLitePullConsumerImpl litePullConsumerImpl) {
        this(null, null, null, null, litePullConsumerImpl);
    }

    public RebalanceLitePullImpl(String consumerGroup, MessageModel messageModel,
        AllocateMessageQueueStrategy allocateMessageQueueStrategy,
        MQClientInstance mQClientFactory, DefaultLitePullConsumerImpl litePullConsumerImpl) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
        this.litePullConsumerImpl = litePullConsumerImpl;
    }
    /**
     * 获取队列监听器 通知 消息队列 发生改变
     * @param topic
     * @param mqAll
     * @param mqDivided
     */
    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
        MessageQueueListener messageQueueListener = this.litePullConsumerImpl.getDefaultLitePullConsumer().getMessageQueueListener();
        if (messageQueueListener != null) {
            try {
                messageQueueListener.messageQueueChanged(topic, mqAll, mqDivided);
            } catch (Throwable e) {
                log.error("messageQueueChanged exception", e);
            }
        }
    }

    /**
     * 根据存储 先保存 偏移量 再移除 偏移量
     * @param mq
     * @param pq
     * @return
     */
    @Override
    public boolean removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq) {
        //FIXME:: 根据存储 先保存 偏移量 再移除 偏移量
        this.litePullConsumerImpl.getOffsetStore().persist(mq);
        this.litePullConsumerImpl.getOffsetStore().removeOffset(mq);
        return true;
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_ACTIVELY;
    }

    @Override
    public void removeDirtyOffset(final MessageQueue mq) {
        this.litePullConsumerImpl.getOffsetStore().removeOffset(mq);
    }

    @Deprecated
    @Override
    public long computePullFromWhere(MessageQueue mq) {
        long result = -1L;
        try {
            result = computePullFromWhereWithException(mq);
        } catch (MQClientException e) {
            log.warn("Compute consume offset exception, mq={}", mq);
        }
        return result;
    }

    @Override
    public long computePullFromWhereWithException(MessageQueue mq) throws MQClientException {
        //获取消费从 何处消费
        ConsumeFromWhere consumeFromWhere = litePullConsumerImpl.getDefaultLitePullConsumer().getConsumeFromWhere();
        long result = -1;
        switch (consumeFromWhere) {
            //从最近的偏移量进行消费 从 store 获取 先从内存读取然后再从磁盘读取 偏移量 如果不存在 则 从该消息队列的broker 获取的最大偏移量
            case CONSUME_FROM_LAST_OFFSET: {
                long lastOffset = litePullConsumerImpl.getOffsetStore().readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                } else if (-1 == lastOffset) {
                    //如果该偏移是-1 并且 该 topic 是 %RETRY% FIXME:: 第一次启动 没有偏移量 ??
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) { // First start, no offset
                        result = 0L;
                    } else {
                        try {
                            //则 从该消息队列的broker 获取的最大偏移量
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            log.warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
                            throw e;
                        }
                    }
                } else {
                    result = -1;
                }
                break;
            }
            case CONSUME_FROM_FIRST_OFFSET: {
                //从 store 获取 先从内存读取然后再从磁盘读取 偏移量
                long lastOffset = litePullConsumerImpl.getOffsetStore().readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                } else if (-1 == lastOffset) {
                    result = 0L;
                } else {
                    result = -1;
                }
                break;
            }
            case CONSUME_FROM_TIMESTAMP: {
                //从 store 获取 先从内存读取然后再从磁盘读取 偏移量
                long lastOffset = litePullConsumerImpl.getOffsetStore().readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                } else if (-1 == lastOffset) {
                    //如果该偏移是-1 并且 该 topic 是 %RETRY%  则 从该消息队列的broker 获取的最大偏移量
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        try {
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            log.warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
                            throw e;
                        }
                    } else {
                        try {
                            //将消费者消费时间转成时间戳
                            long timestamp = UtilAll.parseDate(this.litePullConsumerImpl.getDefaultLitePullConsumer().getConsumeTimestamp(),
                                UtilAll.YYYYMMDDHHMMSS).getTime();
                            //查询 指定时间 消费队列的偏移量
                            result = this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
                        } catch (MQClientException e) {
                            log.warn("Compute consume offset from last offset exception, mq={}, exception={}", mq, e);
                            throw e;
                        }
                    }
                } else {
                    result = -1;
                }
                break;
            }
        }
        return result;
    }

    @Override
    public int getConsumeInitMode() {
        throw new UnsupportedOperationException("no initMode for Pull");
    }

    @Override
    public void dispatchPullRequest(final List<PullRequest> pullRequestList, final long delay) {
    }

    @Override
    public void dispatchPopPullRequest(List<PopRequest> pullRequestList, long delay) {

    }

    @Override
    public ProcessQueue createProcessQueue() {
        return new ProcessQueue();
    }

    @Override
    public PopProcessQueue createPopProcessQueue() {
        return null;
    }

    public ProcessQueue createProcessQueue(String topicName) {
        return createProcessQueue();
    }
}
