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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;

/**
 * MQ的故障策略
 */
public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();
    /**
     * 是否发送的Broker故障延迟
     */
    private boolean sendLatencyFaultEnable = false;
    /**
     * 50 => 0
     * 100 => 0
     * 550 =>30(秒)
     * 1000 =>60(1分钟)
     * 2000 =>120(2分钟)
     * 3000 =>180(3分钟)
     * 15000 =>600(10分钟)
     */
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    /**
     * 0,0,30(秒),60(1分钟),120(2分钟),180(3分钟),600(10分钟)
     */
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * 选着一个队列 不是最近 的 Broker
     * @param tpInfo
     * @param lastBrokerName
     * @return
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        if (this.sendLatencyFaultEnable) {
            try {
                int index = tpInfo.getSendWhichQueue().incrementAndGet();
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    //求余 选着一个发送的队列 该broker 已经结束延迟 则返回该 MessageQueue
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName()))
                        return mq;
                }
                //如果 都没有 没有一个可用 MessageQueue 选着最早的 一个 Broker (可能是已经可用的)
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                //遍历 topic 当中的队列 获取 一个 broker 下的队列 该写队列数量 >0
                //FIXME:: 这一步 是在做 什么?
                //该写队列数量 >0 求余 选着一个发送的队列 将该 MessageQueue 的 brokerName 设置 最早 和 队列 Id 发送的求余
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().incrementAndGet() % writeQueueNums);
                    }
                    return mq;
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }
            //求余 选着一个发送的队
            return tpInfo.selectOneMessageQueue();
        }
        //求余 选着一个发送的队列 该队列的 brokerName 不是最近 发送过的
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * 更新该broker是否需要进行隔离 isolation 为 true 则至少 隔离 3分钟 否则取决 于发送消息的花费时间 currentLatency
     * @param brokerName
     * @param currentLatency
     * @param isolation
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            //计算不可用持续时间
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            //更新该 broker 故障信息
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    /**
     * 计算不可用时间
     * @param currentLatency
     * @return
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
