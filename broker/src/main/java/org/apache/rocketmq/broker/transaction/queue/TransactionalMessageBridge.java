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
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InnerLoggerFactory;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionalMessageBridge {
    private static final InternalLogger LOGGER = InnerLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);
    /**
     * 消息队列的映射  value.topic 为 RMQ_SYS_TRANS_OP_HALF_TOPIC
     */
    private final ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();
    private final BrokerController brokerController;
    /**
     * 消息存储
     */
    private final MessageStore store;
    /**
     * 存储地址
     */
    private final SocketAddress storeHost;

    public TransactionalMessageBridge(BrokerController brokerController, MessageStore store) {
        try {
            this.brokerController = brokerController;
            this.store = store;
            this.storeHost =
                new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(),
                    brokerController.getNettyServerConfig().getListenPort());
        } catch (Exception e) {
            LOGGER.error("Init TransactionBridge error", e);
            throw new RuntimeException(e);
        }

    }

    /**
     * 获取该消息组 CID_RMQ_SYS_TRANS 的偏移量
     * 不存在 则从存储获取 该topic 该消息队列 最小偏移量
     * @param mq
     * @return
     */
    public long fetchConsumeOffset(MessageQueue mq) {
        //获取 该topic CID_RMQ_SYS_TRANS 消费组的 消息队列 获取 偏移量 如果 不存在 则从存储获取 该topic 该消息队列 最小偏移量
        long offset = brokerController.getConsumerOffsetManager().queryOffset(TransactionalMessageUtil.buildConsumerGroup(),
            mq.getTopic(), mq.getQueueId());
        if (offset == -1) {
            offset = store.getMinOffsetInQueue(mq.getTopic(), mq.getQueueId());
        }
        return offset;
    }

    /**
     * 根据 topic 可读数量 获取 MessageQueue 消息队列
     * @param topic
     * @return
     */
    public Set<MessageQueue> fetchMessageQueues(String topic) {
        Set<MessageQueue> mqSet = new HashSet<>();
        // 获取 该 topic 的 配置 如果不存在 则创建 该 topic 配置
        TopicConfig topicConfig = selectTopicConfig(topic);
        //有可读消息队列数量 根据可读数量 获取 MessageQueue 消息队列
        if (topicConfig != null && topicConfig.getReadQueueNums() > 0) {
            for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
                MessageQueue mq = new MessageQueue();
                mq.setTopic(topic);
                mq.setBrokerName(brokerController.getBrokerConfig().getBrokerName());
                mq.setQueueId(i);
                mqSet.add(mq);
            }
        }
        return mqSet;
    }

    public void updateConsumeOffset(MessageQueue mq, long offset) {
        //RemotingHelper.parseSocketAddressAddr(this.storeHost) IP:port FIXME::那这样不是换台 Ip 会变
        //更新 CID_SYS_RMQ_TRANS  该 topic  该消息队列的偏移量
        this.brokerController.getConsumerOffsetManager().commitOffset(
            RemotingHelper.parseSocketAddressAddr(this.storeHost), TransactionalMessageUtil.buildConsumerGroup(), mq.getTopic(),
            mq.getQueueId(), offset);
    }

    /**
     * 从 RMQ_SYS_TRANS_HALF_TOPIC topic 获取 该 CID_SYS_RMQ_TRANS 消费者 该  queueId  消息
     * @param queueId
     * @param offset
     * @param nums
     * @return
     */
    public PullResult getHalfMessage(int queueId, long offset, int nums) {
        String group = TransactionalMessageUtil.buildConsumerGroup();
        String topic = TransactionalMessageUtil.buildHalfTopic();
        SubscriptionData sub = new SubscriptionData(topic, "*");
        return getMessage(group, topic, queueId, offset, nums, sub);
    }

    /**
     * 从 RMQ_SYS_TRANS_OP_HALF_TOPIC topic 获取 该 CID_SYS_RMQ_TRANS 消费者 该  queueId  消息
     * @param queueId
     * @param offset
     * @param nums
     * @return
     */
    public PullResult getOpMessage(int queueId, long offset, int nums) {
        String group = TransactionalMessageUtil.buildConsumerGroup();
        String topic = TransactionalMessageUtil.buildOpTopic();
        SubscriptionData sub = new SubscriptionData(topic, "*");
        return getMessage(group, topic, queueId, offset, nums, sub);
    }

    /**
     * 获取消息
     * @param group
     * @param topic
     * @param queueId
     * @param offset
     * @param nums
     * @param sub
     * @return
     */
    private PullResult getMessage(String group, String topic, int queueId, long offset, int nums,
        SubscriptionData sub) {
        //获取 该组的 该topic 从 该消息队列 获取  nums 消息
        GetMessageResult getMessageResult = store.getMessage(group, topic, queueId, offset, nums, null);

        if (getMessageResult != null) {
            PullStatus pullStatus = PullStatus.NO_NEW_MSG;
            List<MessageExt> foundList = null;
            switch (getMessageResult.getStatus()) {
                case FOUND:
                    //发现消息
                    pullStatus = PullStatus.FOUND;
                    //进行解码
                    foundList = decodeMsgList(getMessageResult);
                    //统计信息
                    this.brokerController.getBrokerStatsManager().incGroupGetNums(group, topic,
                        getMessageResult.getMessageCount());
                    this.brokerController.getBrokerStatsManager().incGroupGetSize(group, topic,
                        getMessageResult.getBufferTotalSize());
                    this.brokerController.getBrokerStatsManager().incBrokerGetNums(getMessageResult.getMessageCount());
                    if (foundList == null || foundList.size() == 0) {
                        break;
                    }
                    this.brokerController.getBrokerStatsManager().recordDiskFallBehindTime(group, topic, queueId,
                        this.brokerController.getMessageStore().now() - foundList.get(foundList.size() - 1)
                            .getStoreTimestamp());
                    break;
                case NO_MATCHED_MESSAGE:
                    //没有匹配的消息
                    pullStatus = PullStatus.NO_MATCHED_MSG;
                    LOGGER.warn("No matched message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                case NO_MESSAGE_IN_QUEUE:
                case OFFSET_OVERFLOW_ONE:
                    //没有新消息
                    pullStatus = PullStatus.NO_NEW_MSG;
                    LOGGER.warn("No new message. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                case MESSAGE_WAS_REMOVING:
                case NO_MATCHED_LOGIC_QUEUE:
                case OFFSET_FOUND_NULL:
                case OFFSET_OVERFLOW_BADLY:
                case OFFSET_TOO_SMALL:
                    //非法的偏移量
                    pullStatus = PullStatus.OFFSET_ILLEGAL;
                    LOGGER.warn("Offset illegal. GetMessageStatus={}, topic={}, groupId={}, requestOffset={}",
                        getMessageResult.getStatus(), topic, group, offset);
                    break;
                default:
                    assert false;
                    break;
            }
            //返回获取的的结果
            return new PullResult(pullStatus, getMessageResult.getNextBeginOffset(), getMessageResult.getMinOffset(),
                getMessageResult.getMaxOffset(), foundList);

        } else {
            LOGGER.error("Get message from store return null. topic={}, groupId={}, requestOffset={}", topic, group,
                offset);
            return null;
        }
    }

    /**
     * 对获得的消息进行解码
     * @param getMessageResult
     * @return
     */
    private List<MessageExt> decodeMsgList(GetMessageResult getMessageResult) {
        List<MessageExt> foundList = new ArrayList<>();
        try {
            //遍历发现的消息进解码
            List<ByteBuffer> messageBufferList = getMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {
                MessageExt msgExt = MessageDecoder.decode(bb, true, false);
                if (msgExt != null) {
                    foundList.add(msgExt);
                }
            }

        } finally {
            getMessageResult.release();
        }

        return foundList;
    }

    /**
     * 同步存储半事务消息
     * @param messageInner
     * @return
     */
    public PutMessageResult putHalfMessage(MessageExtBrokerInner messageInner) {
        return store.putMessage(parseHalfMessageInner(messageInner));
    }
    /**
     * 异步存储半事务消息
     * @param messageInner
     * @return
     */
    public CompletableFuture<PutMessageResult> asyncPutHalfMessage(MessageExtBrokerInner messageInner) {
        return store.asyncPutMessage(parseHalfMessageInner(messageInner));
    }

    private MessageExtBrokerInner parseHalfMessageInner(MessageExtBrokerInner msgInner) {
        //添加 REAL_TOPIC REAL_QID 属性
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC, msgInner.getTopic());
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID,
            String.valueOf(msgInner.getQueueId()));
        //FIXME::重置事务标记 TRANSACTION_NOT_TYPE 去掉事务标记?
        msgInner.setSysFlag(
            MessageSysFlag.resetTransactionValue(msgInner.getSysFlag(), MessageSysFlag.TRANSACTION_NOT_TYPE));
        //设置 topic 为 "RMQ_SYS_TRANS_HALF_TOPIC"
        msgInner.setTopic(TransactionalMessageUtil.buildHalfTopic());
        msgInner.setQueueId(0);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        return msgInner;
    }

    public boolean putOpMessage(MessageExt messageExt, String opType) {
        MessageQueue messageQueue = new MessageQueue(messageExt.getTopic(),
            this.brokerController.getBrokerConfig().getBrokerName(), messageExt.getQueueId());
        if (TransactionalMessageUtil.REMOVETAG.equals(opType)) {
            return addRemoveTagInTransactionOp(messageExt, messageQueue);
        }
        return true;
    }

    /**
     * 存储消息返回结果
     * @param messageInner
     * @return
     */
    public PutMessageResult putMessageReturnResult(MessageExtBrokerInner messageInner) {
        LOGGER.debug("[BUG-TO-FIX] Thread:{} msgID:{}", Thread.currentThread().getName(), messageInner.getMsgId());
        return store.putMessage(messageInner);
    }

    /**
     * 存储消息返回是否成功
     * @param messageInner
     * @return
     */
    public boolean putMessage(MessageExtBrokerInner messageInner) {
        PutMessageResult putMessageResult = store.putMessage(messageInner);
        if (putMessageResult != null
            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            return true;
        } else {
            LOGGER.error("Put message failed, topic: {}, queueId: {}, msgId: {}",
                messageInner.getTopic(), messageInner.getQueueId(), messageInner.getMsgId());
            return false;
        }
    }

    public MessageExtBrokerInner renewImmunityHalfMessageInner(MessageExt msgExt) {
        //将消息转成 MessageExtBrokerInner
        MessageExtBrokerInner msgInner = renewHalfMessageInner(msgExt);
        String queueOffsetFromPrepare = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        // 复制 原消息的 TRAN_PREPARED_QUEUE_OFFSET 不存在 则 以 原消息的队列偏移量
        if (null != queueOffsetFromPrepare) {
            MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET,
                String.valueOf(queueOffsetFromPrepare));
        } else {
            MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET,
                String.valueOf(msgExt.getQueueOffset()));
        }

        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        return msgInner;
    }

    public MessageExtBrokerInner renewHalfMessageInner(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(msgExt.getTopic());
        msgInner.setBody(msgExt.getBody());
        msgInner.setQueueId(msgExt.getQueueId());
        msgInner.setMsgId(msgExt.getMsgId());
        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setTags(msgExt.getTags());
        // 将tags 转成 hash编码
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(msgInner.getTags()));
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setWaitStoreMsgOK(false);
        return msgInner;
    }

    private MessageExtBrokerInner makeOpMessageInner(Message message, MessageQueue messageQueue) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(message.getTopic());
        msgInner.setBody(message.getBody());
        //消息 的 队列 id 设置成消息队列 id
        msgInner.setQueueId(messageQueue.getQueueId());
        msgInner.setTags(message.getTags());
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(msgInner.getTags()));
        //sysFlag 为 0
        msgInner.setSysFlag(0);
        MessageAccessor.setProperties(msgInner, message.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(message.getProperties()));
        msgInner.setBornTimestamp(System.currentTimeMillis());
        msgInner.setBornHost(this.storeHost);
        msgInner.setStoreHost(this.storeHost);
        msgInner.setWaitStoreMsgOK(false);
        //设置唯一id
        MessageClientIDSetter.setUniqID(msgInner);
        return msgInner;
    }

    /**
     * 获取 该 topic 的 配置 如果不存在 则创建 该 topic 配置
     * @param topic
     * @return
     */
    private TopicConfig selectTopicConfig(String topic) {
        TopicConfig topicConfig = brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (topicConfig == null) {
            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                topic, 1, PermName.PERM_WRITE | PermName.PERM_READ, 0);
        }
        return topicConfig;
    }

    /**
     * Use this function while transaction msg is committed or rollback write a flag 'd' to operation queue for the
     * msg's offset
     *
     * @param prepareMessage Half message
     * @param messageQueue Half message queue
     * @return This method will always return true.
     */
    private boolean addRemoveTagInTransactionOp(MessageExt prepareMessage, MessageQueue messageQueue) {
        //将消息 Tag 设置成 REMOVETAG "d", body 为 队列的偏移量
        Message message = new Message(TransactionalMessageUtil.buildOpTopic(), TransactionalMessageUtil.REMOVETAG,
            String.valueOf(prepareMessage.getQueueOffset()).getBytes(TransactionalMessageUtil.CHARSET));
        writeOp(message, messageQueue);
        return true;
    }

    private void writeOp(Message message, MessageQueue mq) {
        MessageQueue opQueue;
        //根据消息队列 获取 opQueue
        if (opQueueMap.containsKey(mq)) {
            opQueue = opQueueMap.get(mq);
        } else {
            //不存在进行新的映射
            opQueue = getOpQueueByHalf(mq);
            MessageQueue oldQueue = opQueueMap.putIfAbsent(mq, opQueue);
            if (oldQueue != null) {
                opQueue = oldQueue;
            }
        }
        if (opQueue == null) {
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), mq.getBrokerName(), mq.getQueueId());
        }
        //进行存储 topic 为 RMQ_SYS_TRANS_OP_HALF_TOPIC
        putMessage(makeOpMessageInner(message, opQueue));
    }

    /**
     * 将消息队列 抓成 OpQueue topic 设置成 RMQ_SYS_TRANS_OP_HALF_TOPIC
     * @param halfMQ
     * @return
     */
    private MessageQueue getOpQueueByHalf(MessageQueue halfMQ) {
        MessageQueue opQueue = new MessageQueue();
        opQueue.setTopic(TransactionalMessageUtil.buildOpTopic());
        opQueue.setBrokerName(halfMQ.getBrokerName());
        opQueue.setQueueId(halfMQ.getQueueId());
        return opQueue;
    }

    /**
     * 根据偏移量查找消息
     * @param commitLogOffset
     * @return
     */
    public MessageExt lookMessageByOffset(final long commitLogOffset) {
        return this.store.lookMessageByOffset(commitLogOffset);
    }

    public BrokerController getBrokerController() {
        return brokerController;
    }
}
