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

import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 将超过最大次的数回查事务状态的消息 进行存储 记录日志
 */
public class DefaultTransactionalMessageCheckListener extends AbstractTransactionalMessageCheckListener {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    public DefaultTransactionalMessageCheckListener() {
        super();
    }

    /**
     * 为了避免无限检查，我们将丢弃已检查超过一定次数的消息。 处理丢弃的消息
     * @param msgExt Message to be discarded.
     */
    @Override
    public void resolveDiscardMsg(MessageExt msgExt) {
        log.error("MsgExt:{} has been checked too many times, so discard it by moving it to system topic TRANS_CHECK_MAXTIME_TOPIC", msgExt);

        try {
            //将消息转成 MessageExtBrokerInner
            MessageExtBrokerInner brokerInner = toMessageExtBrokerInner(msgExt);
            //进行存储
            PutMessageResult putMessageResult = this.getBrokerController().getMessageStore().putMessage(brokerInner);
            //存储成功 日志输出
            if (putMessageResult != null && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                log.info("Put checked-too-many-time half message to TRANS_CHECK_MAXTIME_TOPIC OK. Restored in queueOffset={}, " +
                    "commitLogOffset={}, real topic={}", msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
            } else {
                log.error("Put checked-too-many-time half message to TRANS_CHECK_MAXTIME_TOPIC failed, real topic={}, msgId={}", msgExt.getTopic(), msgExt.getMsgId());
            }
        } catch (Exception e) {
            log.warn("Put checked-too-many-time message to TRANS_CHECK_MAXTIME_TOPIC error. {}", e);
        }

    }

    private MessageExtBrokerInner toMessageExtBrokerInner(MessageExt msgExt) {
        //创建 事务检查最大的次数(RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC) 的 topic 配置
        TopicConfig topicConfig = this.getBrokerController().getTopicConfigManager().createTopicOfTranCheckMaxTime(TCMT_QUEUE_NUMS, PermName.PERM_READ | PermName.PERM_WRITE);
        int queueId = ThreadLocalRandom.current().nextInt(99999999) % TCMT_QUEUE_NUMS;
        MessageExtBrokerInner inner = new MessageExtBrokerInner();
        inner.setTopic(topicConfig.getTopicName());
        inner.setBody(msgExt.getBody());
        inner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(inner, msgExt.getProperties());
        inner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        inner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(msgExt.getTags()));
        inner.setQueueId(queueId);
        inner.setSysFlag(msgExt.getSysFlag());
        inner.setBornHost(msgExt.getBornHost());
        inner.setBornTimestamp(msgExt.getBornTimestamp());
        inner.setStoreHost(msgExt.getStoreHost());
        inner.setReconsumeTimes(msgExt.getReconsumeTimes());
        inner.setMsgId(msgExt.getMsgId());
        inner.setWaitStoreMsgOK(false);
        return inner;
    }
}
