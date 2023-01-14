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
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.consumer.AckCallback;
import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class ConsumeMessagePopConcurrentlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    /**
     * 消息监听者批量 消费消息
     */
    private final MessageListenerConcurrently messageListener;
    /**
     * 消费消息请求对列
     */
    private final BlockingQueue<Runnable> consumeRequestQueue;
    /**
     * 消费线程池
     */
    private final ThreadPoolExecutor consumeExecutor;
    /**
     * 消费者组
     */
    private final String consumerGroup;
    /**
     * 单线程 用来延迟执行 提交请求
     */
    private final ScheduledExecutorService scheduledExecutorService;

    public ConsumeMessagePopConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerConcurrently messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        this.consumeExecutor = new ThreadPoolExecutor(
            this.defaultMQPushConsumer.getConsumeThreadMin(),
            this.defaultMQPushConsumer.getConsumeThreadMax(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.consumeRequestQueue,
            new ThreadFactoryImpl("ConsumeMessageThread_"));

        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
    }

    public void start() {
    }

    public void shutdown(long awaitTerminateMillis) {
        this.scheduledExecutorService.shutdown();
        ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0
            && corePoolSize <= Short.MAX_VALUE
            && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {
    }

    @Override
    public void decCorePoolSize() {
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }


    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(false);
        result.setAutoCommit(true);

        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);
        //重置消息的 topic
        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            //消费监听者进行消费
            ConsumeConcurrentlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case CONSUME_SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case RECONSUME_LATER:
                        result.setConsumeResult(CMResult.CR_LATER);
                        break;
                    default:
                        break;
                }
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(RemotingHelper.exceptionSimpleDesc(e));

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
                RemotingHelper.exceptionSimpleDesc(e),
                ConsumeMessagePopConcurrentlyService.this.consumerGroup,
                msgs,
                mq), e);
        }

        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    @Override
    public void submitConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue,
                                     MessageQueue messageQueue, boolean dispathToConsume) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void submitPopConsumeRequest(
        final List<MessageExt> msgs,
        final PopProcessQueue processQueue,
        final MessageQueue messageQueue) {
        //获取消费者批量消费消息大小
        final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();
        //如果消息数量 小于  批量消费消息大小 则提交 消费请求 失败 延迟提交消费请求
        if (msgs.size() <= consumeBatchSize) {
            ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
            try {
                this.consumeExecutor.submit(consumeRequest);
            } catch (RejectedExecutionException e) {
                this.submitConsumeRequestLater(consumeRequest);
            }
        } else {
            //如果消息数量 大于  批量消费消息大小
            for (int total = 0; total < msgs.size(); ) {
                //将消息进行分批 然后批量 提交消费请求
                List<MessageExt> msgThis = new ArrayList<MessageExt>(consumeBatchSize);
                for (int i = 0; i < consumeBatchSize; i++, total++) {
                    if (total < msgs.size()) {
                        msgThis.add(msgs.get(total));
                    } else {
                        break;
                    }
                }

                ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
                try {
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    for (; total < msgs.size(); total++) {
                        msgThis.add(msgs.get(total));
                    }

                    this.submitConsumeRequestLater(consumeRequest);
                }
            }
        }
    }

    /**
     * 处理消费结果 消费成功 进行 ack 确认 消费失败 根据重试次 将消息状态 变的 不见  超过最大 重试 次数 进行 ack 确认
     * @param status
     * @param context
     * @param consumeRequest
     */
    public void processConsumeResult(
        final ConsumeConcurrentlyStatus status,
        final ConsumeConcurrentlyContext context,
        final ConsumeRequest consumeRequest) {

        if (consumeRequest.getMsgs().isEmpty()) {
            return;
        }

        int ackIndex = context.getAckIndex();
        String topic = consumeRequest.getMessageQueue().getTopic();

        switch (status) {
            case CONSUME_SUCCESS:
                // ack index 大于消息 数量 则将 ackIndex 设为消息 数量
                if (ackIndex >= consumeRequest.getMsgs().size()) {
                    ackIndex = consumeRequest.getMsgs().size() - 1;
                }
                //成功 ack 的消息 数量
                int ok = ackIndex + 1;
                //失败的数量
                int failed = consumeRequest.getMsgs().size() - ok;
                //统计计数
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, topic, ok);
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, topic, failed);
                break;
            case RECONSUME_LATER:
                ackIndex = -1;
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, topic,
                        consumeRequest.getMsgs().size());
                break;
            default:
                break;
        }

        //ack if consume success
        //成功消费 发送异步 ack 确认 等待 ack 计数
        for (int i = 0; i <= ackIndex; i++) {
            this.defaultMQPushConsumerImpl.ackAsync(consumeRequest.getMsgs().get(i), consumerGroup);
            consumeRequest.getPopProcessQueue().ack();
        }

        //consume later if consume fail
        //失败的消费 遍历是超过 最大消费次数 减少ack 确认数量
        for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
            MessageExt msgExt = consumeRequest.getMsgs().get(i);
            consumeRequest.getPopProcessQueue().ack();
            //如果超过最大重试次 如果延迟时间最大延迟时间2倍则 ack 确认 否则 计算 延迟等 将消息 变得不可见
            if (msgExt.getReconsumeTimes() >= this.defaultMQPushConsumerImpl.getMaxReconsumeTimes()) {
                checkNeedAckOrDelay(msgExt);
                continue;
            }
            // 计算 延迟等 将消息 变得不可见
            int delayLevel = context.getDelayLevelWhenNextConsume();
            changePopInvisibleTime(consumeRequest.getMsgs().get(i), consumerGroup, delayLevel);
        }
    }

    /**
     * 如果延迟时间最大延迟时间2倍则 ack 确认 FIXME:: 应该 会投入到 DLQ 吧
     * 否则 计算 延迟等 将消息 变得不可见
     * @param msgExt
     */
    private void checkNeedAckOrDelay(MessageExt msgExt) {

        int[] delayLevelTable = this.defaultMQPushConsumerImpl.getPopDelayLevel();
        //计算消息消费生产到现在的时间差 如果超过 最大延迟时间的两倍 则消费 次数 太多  直接 确认 该消息
        long msgDelaytime = System.currentTimeMillis() - msgExt.getBornTimestamp();
        if (msgDelaytime > delayLevelTable[delayLevelTable.length - 1] * 1000 * 2) {
            log.warn("Consume too many times, ack message async. message {}", msgExt.toString());
            this.defaultMQPushConsumerImpl.ackAsync(msgExt, consumerGroup);
        } else {
            //遍历延迟时间等级表计算 延迟等级
            int delayLevel = delayLevelTable.length - 1;
            for (; delayLevel >= 0; delayLevel--) {
                if (msgDelaytime >= delayLevelTable[delayLevel] * 1000) {
                    delayLevel++;
                    break;
                }
            }
            //更改消息不可见时间
            changePopInvisibleTime(msgExt, consumerGroup, delayLevel);
            log.warn("Consume too many times, but delay time {} not enough. changePopInvisibleTime to delayLevel {} . message key:{}",
                msgDelaytime, delayLevel, msgExt.getKeys());
        }
    }

    /**
     * 更改消息不可见时间
     * @param msg
     * @param consumerGroup
     * @param delayLevel
     */
    private void changePopInvisibleTime(final MessageExt msg, String consumerGroup, int delayLevel) {
        //若果延迟等级是0 则延迟等级 按照消费重新消费次数来
        if (0 == delayLevel) {
            delayLevel = msg.getReconsumeTimes();
        }

        int[] delayLevelTable = this.defaultMQPushConsumerImpl.getPopDelayLevel();
        //计算延迟时间 多少秒
        int delaySecond = delayLevel >= delayLevelTable.length ? delayLevelTable[delayLevelTable.length - 1] : delayLevelTable[delayLevel];
        String extraInfo = msg.getProperty(MessageConst.PROPERTY_POP_CK);
        //更改下消息 不可见的时间
        try {
            this.defaultMQPushConsumerImpl.changePopInvisibleTimeAsync(msg.getTopic(), consumerGroup, extraInfo,
                    delaySecond * 1000L, new AckCallback() {
                        @Override
                        public void onSuccess(AckResult ackResult) {
                        }


                        @Override
                        public void onException(Throwable e) {
                            log.error("changePopInvisibleTimeAsync fail. msg:{} error info: {}", msg.toString(), e.toString());
                        }
                    });
        } catch (Throwable t) {
            log.error("changePopInvisibleTimeAsync fail, group:{} msg:{} errorInfo:{}", consumerGroup, msg.toString(), t.toString());
        }
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    /**
     * 延迟提交 消费请求
     * @param msgs
     * @param processQueue
     * @param messageQueue
     */
    private void submitConsumeRequestLater(
        final List<MessageExt> msgs,
        final PopProcessQueue processQueue,
        final MessageQueue messageQueue
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessagePopConcurrentlyService.this.submitPopConsumeRequest(msgs, processQueue, messageQueue);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    /**
     * 延迟提交 消费请求
     * @param consumeRequest
     */
    private void submitConsumeRequestLater(final ConsumeRequest consumeRequest
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessagePopConcurrentlyService.this.consumeExecutor.submit(consumeRequest);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    class ConsumeRequest implements Runnable {
        /**
         * 消息集合
         */
        private final List<MessageExt> msgs;
        /**
         * pop处理队列
         */
        private final PopProcessQueue processQueue;
        /**
         * 消息队列
         */
        private final MessageQueue messageQueue;
        private long popTime = 0;
        private long invisibleTime = 0;

        public ConsumeRequest(List<MessageExt> msgs, PopProcessQueue processQueue, MessageQueue messageQueue) {
            this.msgs = msgs;
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;

            try {
                //获取雄安县的额外信息
                String extraInfo = msgs.get(0).getProperty(MessageConst.PROPERTY_POP_CK);
                String[] extraInfoStrs = ExtraInfoUtil.split(extraInfo);
                //获取 弹出时间戳
                popTime = ExtraInfoUtil.getPopTime(extraInfoStrs);
                //获取消息的不可见时间
                invisibleTime = ExtraInfoUtil.getInvisibleTime(extraInfoStrs);
            } catch (Throwable t) {
                log.error("parse extra info error. msg:" + msgs.get(0), t);
            }
        }

        /**
         * 弹出时间超时 当前时间 -弹出时间 > 不看见时间 也就 意味 不可见时间超时
         * @return
         */
        public boolean isPopTimeout() {
            if (msgs.size() == 0 || popTime <= 0 || invisibleTime <= 0) {
                return true;
            }

            long current = System.currentTimeMillis();
            return current - popTime >= invisibleTime;
        }

        public List<MessageExt> getMsgs() {
            return msgs;
        }

        public PopProcessQueue getPopProcessQueue() {
            return processQueue;
        }

        /**
         * 进行消费
         */
        @Override
        public void run() {
            if (this.processQueue.isDropped()) {
                log.info("the message queue not be able to consume, because it's dropped(pop). group={} {}", ConsumeMessagePopConcurrentlyService.this.consumerGroup, this.messageQueue);
                return;
            }
            //超出消费时间
            if (isPopTimeout()) {
                log.info("the pop message time out so abort consume. popTime={} invisibleTime={}, group={} {}",
                        popTime, invisibleTime, ConsumeMessagePopConcurrentlyService.this.consumerGroup, this.messageQueue);
                processQueue.decFoundMsg(-msgs.size());
                return;
            }
            //消息监听者
            MessageListenerConcurrently listener = ConsumeMessagePopConcurrentlyService.this.messageListener;
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);
            ConsumeConcurrentlyStatus status = null;
            //重置消息的 topic
            defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

            ConsumeMessageContext consumeMessageContext = null;
            //构建 消费消息的 context 执行消费前置钩子
            if (ConsumeMessagePopConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
                consumeMessageContext.setProps(new HashMap<String, String>());
                consumeMessageContext.setMq(messageQueue);
                consumeMessageContext.setMsgList(msgs);
                consumeMessageContext.setSuccess(false);
                ConsumeMessagePopConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
            }

            long beginTimestamp = System.currentTimeMillis();
            boolean hasException = false;
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
            try {
                //遍历设置消息的消费开始时间
                if (msgs != null && !msgs.isEmpty()) {
                    for (MessageExt msg : msgs) {
                        MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
                    }
                }
                //监听器消费消息
                status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);
            } catch (Throwable e) {
                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                    RemotingHelper.exceptionSimpleDesc(e),
                    ConsumeMessagePopConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
                hasException = true;
            }
            //计算消费响应时间
            long consumeRT = System.currentTimeMillis() - beginTimestamp;
            //消费状态 是 null
            if (null == status) {
                //发生异常 设置 消费 发生异常
                if (hasException) {
                    returnType = ConsumeReturnType.EXCEPTION;
                } else {
                    returnType = ConsumeReturnType.RETURNNULL;
                }
            } else if (consumeRT >= invisibleTime * 1000) {
                //如果消息时间消息时间 超过不可见 时间 则 消费超时
                returnType = ConsumeReturnType.TIME_OUT;
            } else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
                //如果消费状态是消费失败 待会重试 则返回消息消费失败
                returnType = ConsumeReturnType.FAILED;
            } else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {
                //如果消费状态是消费成功 则返回消息消费成功
                returnType = ConsumeReturnType.SUCCESS;
            }
            //如果消费状态 是 null 则将装设置  RECONSUME_LATER
            if (null == status) {
                log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",
                    ConsumeMessagePopConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
            //消费消息context 设置 结果 执行消费后置钩子
            if (ConsumeMessagePopConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
                consumeMessageContext.setStatus(status.toString());
                consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                ConsumeMessagePopConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
            }
            //增加该消费组topic的消费时间计数
            ConsumeMessagePopConcurrentlyService.this.getConsumerStatsManager()
                .incConsumeRT(ConsumeMessagePopConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);
            //如果消费队列没有 被丢弃 并且 弹出消息 没有超时 则处理消费结果
            //消费成功 进行 ack 确认 消费失败 根据重试次 将消息状态 变的 不见  超过最大 重试 次数 进行 ack 确认
            if (!processQueue.isDropped() && !isPopTimeout()) {
                ConsumeMessagePopConcurrentlyService.this.processConsumeResult(status, context, this);
            } else {
                //减少等待确认的消息数量
                if (msgs != null) {
                    processQueue.decFoundMsg(-msgs.size());
                }

                log.warn("processQueue invalid. isDropped={}, isPopTimeout={}, messageQueue={}, msgs={}",
                        processQueue.isDropped(), isPopTimeout(), messageQueue, msgs);
            }
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

    }
}
