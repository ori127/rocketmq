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
package org.apache.rocketmq.broker.client;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.netty.channel.Channel;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

public class ConsumerManager {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    /**
     * 连接过期时间
     */
    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;
    /**
     * key 为消费组 ,value 为 消费组信息
     */
    private final ConcurrentMap<String, ConsumerGroupInfo> consumerTable =
        new ConcurrentHashMap<String, ConsumerGroupInfo>(1024);
    /**
     * 消费者监听器
     */
    private final List<ConsumerIdsChangeListener> consumerIdsChangeListenerList = new CopyOnWriteArrayList<>();
    /**
     * broker 状态
     */
    protected final BrokerStatsManager brokerStatsManager;

    public ConsumerManager(final ConsumerIdsChangeListener consumerIdsChangeListener) {
        this.consumerIdsChangeListenerList.add(consumerIdsChangeListener);
        this.brokerStatsManager = null;
    }

    public ConsumerManager(final ConsumerIdsChangeListener consumerIdsChangeListener,
        final BrokerStatsManager brokerStatsManager) {
        this.consumerIdsChangeListenerList.add(consumerIdsChangeListener);
        this.brokerStatsManager = brokerStatsManager;
    }

    /**
     * 获取该 消费组的 消费者集合 再根据 客户端id 获取 消费客户端信息
     * @param group
     * @param clientId
     * @return
     */
    public ClientChannelInfo findChannel(final String group, final String clientId) {
        //获取该 消费组的 消费者集合 再根据 客户端id 获取 消费客户端信息
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.findChannel(clientId);
        }
        return null;
    }

    /**
     * 获取该消费组 的消费信息 再获取 该消费组的 该 topic 订阅信息
     * @param group
     * @param topic
     * @return
     */
    public SubscriptionData findSubscriptionData(final String group, final String topic) {
        //获取该消费组的消费信息 再获取 该消费组的 该 topic 订阅信息
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.findSubscriptionData(topic);
        }

        return null;
    }

    public ConcurrentMap<String, ConsumerGroupInfo> getConsumerTable() {
        return this.consumerTable;
    }

    public ConsumerGroupInfo getConsumerGroupInfo(final String group) {
        return this.consumerTable.get(group);
    }
    /**
     * 获取该消费组 的消费信息 订阅信息 数量
     * @param group
     * @param topic
     * @return
     */
    public int findSubscriptionDataCount(final String group) {
        ConsumerGroupInfo consumerGroupInfo = this.getConsumerGroupInfo(group);
        if (consumerGroupInfo != null) {
            return consumerGroupInfo.getSubscriptionTable().size();
        }

        return 0;
    }

    /**
     * 遍历消费组信息 移除 消费客户端的 消费信息 调用客户端取消注册 如果该消费组客户端为空 则移除该消费组 调用客户端改变监听
     * @param remoteAddr
     * @param channel
     * @return
     */
    public boolean doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        boolean removed = false;
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        //遍历消费组信息 移除 消费客户端的 消费信息 调用客户端取消注册 如果该消费组客户端为空 则移除该消费组 调用客户端改变监听
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> next = it.next();
            ConsumerGroupInfo info = next.getValue();
            ClientChannelInfo clientChannelInfo = info.doChannelCloseEvent(remoteAddr, channel);
            if (clientChannelInfo != null) {
                callConsumerIdsChangeListener(ConsumerGroupEvent.CLIENT_UNREGISTER, next.getKey(), clientChannelInfo, info.getSubscribeTopics());
                if (info.getChannelInfoTable().isEmpty()) {
                    ConsumerGroupInfo remove = this.consumerTable.remove(next.getKey());
                    if (remove != null) {
                        LOGGER.info("unregister consumer ok, no any connection, and remove consumer group, {}",
                            next.getKey());
                        callConsumerIdsChangeListener(ConsumerGroupEvent.UNREGISTER, next.getKey());
                    }
                }

                callConsumerIdsChangeListener(ConsumerGroupEvent.CHANGE, next.getKey(), info.getAllChannel());
            }
        }
        return removed;
    }

    /**
     * 注册消费者
     * @param group
     * @param clientChannelInfo
     * @param consumeType
     * @param messageModel
     * @param consumeFromWhere
     * @param subList
     * @param isNotifyConsumerIdsChangedEnable
     * @return
     */
    public boolean registerConsumer(final String group, final ClientChannelInfo clientChannelInfo,
        ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
        final Set<SubscriptionData> subList, boolean isNotifyConsumerIdsChangedEnable) {
        return registerConsumer(group, clientChannelInfo, consumeType, messageModel, consumeFromWhere, subList,
            isNotifyConsumerIdsChangedEnable, true);
    }

    /**
     * 注册消费者 更新消费组信息 添加消费组的 客户端信息 更新消费组的订阅信息
     * @param group
     * @param clientChannelInfo
     * @param consumeType
     * @param messageModel
     * @param consumeFromWhere
     * @param subList
     * @param isNotifyConsumerIdsChangedEnable
     * @param updateSubscription
     * @return
     */
    public boolean registerConsumer(final String group, final ClientChannelInfo clientChannelInfo,
        ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere,
        final Set<SubscriptionData> subList, boolean isNotifyConsumerIdsChangedEnable, boolean updateSubscription) {
        long start = System.currentTimeMillis();
        //根据消费组 获取消费组信息 不存在 创建消费组信息 调用消费监听器 客户端注册
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null == consumerGroupInfo) {
            callConsumerIdsChangeListener(ConsumerGroupEvent.CLIENT_REGISTER, group, clientChannelInfo,
                subList.stream().map(SubscriptionData::getTopic).collect(Collectors.toSet()));
            ConsumerGroupInfo tmp = new ConsumerGroupInfo(group, consumeType, messageModel, consumeFromWhere);
            ConsumerGroupInfo prev = this.consumerTable.putIfAbsent(group, tmp);
            consumerGroupInfo = prev != null ? prev : tmp;
        }
        //更新消费组信息 添加消费组的 客户端信息
        boolean r1 =
            consumerGroupInfo.updateChannel(clientChannelInfo, consumeType, messageModel,
                consumeFromWhere);
        boolean r2 = false;
        //更新消费组的订阅信息 遍历 subList 如果订阅信息 不存在该topic 订阅 则 添加 如果已经存在 则判断判 版本进行修改
        if (updateSubscription) {
            r2 = consumerGroupInfo.updateSubscription(subList);
        }
        //消费者发生该变 订阅信息发生改变,消费者客户端发生改变 通知消费客户端监听
        if (r1 || r2) {
            if (isNotifyConsumerIdsChangedEnable) {
                callConsumerIdsChangeListener(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
            }
        }
        //增加统计
        if (null != this.brokerStatsManager) {
            this.brokerStatsManager.incConsumerRegisterTime((int) (System.currentTimeMillis() - start));
        }
        //通知消费客户端监听 客户端注册
        callConsumerIdsChangeListener(ConsumerGroupEvent.REGISTER, group, subList);

        return r1 || r2;
    }

    public void unregisterConsumer(final String group, final ClientChannelInfo clientChannelInfo,
        boolean isNotifyConsumerIdsChangedEnable) {
        ConsumerGroupInfo consumerGroupInfo = this.consumerTable.get(group);
        if (null != consumerGroupInfo) {
            boolean removed = consumerGroupInfo.unregisterChannel(clientChannelInfo);
            if (removed) {
                callConsumerIdsChangeListener(ConsumerGroupEvent.CLIENT_UNREGISTER, group, clientChannelInfo, consumerGroupInfo.getSubscribeTopics());
            }
            //如果该消费组 消费客户端 为空 则移除 该消费组 调用客户端发生改变监听 消费组取消注册
            if (consumerGroupInfo.getChannelInfoTable().isEmpty()) {
                ConsumerGroupInfo remove = this.consumerTable.remove(group);
                if (remove != null) {
                    LOGGER.info("unregister consumer ok, no any connection, and remove consumer group, {}", group);

                    callConsumerIdsChangeListener(ConsumerGroupEvent.UNREGISTER, group);
                }
            }
            //调用客户端发生改变监听
            if (isNotifyConsumerIdsChangedEnable) {
                callConsumerIdsChangeListener(ConsumerGroupEvent.CHANGE, group, consumerGroupInfo.getAllChannel());
            }
        }
    }

    /**
     * 遍历 consumerTable 每个消费组的 每个消费客户端信息 判断空闲时间是否 超时 调用客户端发生改变 客户端取消注册 关闭该 消费者客户端连接 从该消费组 移除该 客户端
     */
    public void scanNotActiveChannel() {
        //遍历 consumerTable 每个消费组的 每个消费客户端信息 判断空闲时间是否 超时
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> next = it.next();
            String group = next.getKey();
            ConsumerGroupInfo consumerGroupInfo = next.getValue();
            //该消费组的 消费客户端
            ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable =
                consumerGroupInfo.getChannelInfoTable();

            Iterator<Entry<Channel, ClientChannelInfo>> itChannel = channelInfoTable.entrySet().iterator();
            while (itChannel.hasNext()) {
                Entry<Channel, ClientChannelInfo> nextChannel = itChannel.next();
                ClientChannelInfo clientChannelInfo = nextChannel.getValue();
                //消费客户端信息 判断空闲时间是否 超时 调用客户端发生改变 客户端取消注册 关闭该 消费者客户端连接 从该消费组 移除该 客户端
                long diff = System.currentTimeMillis() - clientChannelInfo.getLastUpdateTimestamp();
                if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                    LOGGER.warn(
                        "SCAN: remove expired channel from ConsumerManager consumerTable. channel={}, consumerGroup={}",
                        RemotingHelper.parseChannelRemoteAddr(clientChannelInfo.getChannel()), group);
                    callConsumerIdsChangeListener(ConsumerGroupEvent.CLIENT_UNREGISTER, group, clientChannelInfo, consumerGroupInfo.getSubscribeTopics());
                    RemotingUtil.closeChannel(clientChannelInfo.getChannel());
                    itChannel.remove();
                }
            }
            //如果该消费组 消费客户端 为空 则移除 该消费组
            if (channelInfoTable.isEmpty()) {
                LOGGER.warn(
                    "SCAN: remove expired channel from ConsumerManager consumerTable, all clear, consumerGroup={}",
                    group);
                it.remove();
            }
        }
    }

    /**
     * 获取该 topic 被哪几个消费组 消费
     * @param topic
     * @return
     */
    public HashSet<String> queryTopicConsumeByWho(final String topic) {
        HashSet<String> groups = new HashSet<>();
        //遍历 consumerTable
        Iterator<Entry<String, ConsumerGroupInfo>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConsumerGroupInfo> entry = it.next();
            //获取每个消费消费组的 订阅信息
            ConcurrentMap<String, SubscriptionData> subscriptionTable =
                entry.getValue().getSubscriptionTable();
            if (subscriptionTable.containsKey(topic)) {
                groups.add(entry.getKey());
            }
        }
        return groups;
    }

    public void appendConsumerIdsChangeListener(ConsumerIdsChangeListener listener) {
        consumerIdsChangeListenerList.add(listener);
    }

    /**
     * 消费者客户端发生改变监听器
     * @param event
     * @param group
     * @param args
     */
    protected void callConsumerIdsChangeListener(ConsumerGroupEvent event, String group, Object... args) {
        for (ConsumerIdsChangeListener listener : consumerIdsChangeListenerList) {
            try {
                listener.handle(event, group, args);
            } catch (Throwable t) {
                LOGGER.error("err when call consumerIdsChangeListener", t);
            }
        }
    }
}
