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

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.rocketmq.broker.util.PositiveAtomicCounter;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.body.ProducerInfo;
import org.apache.rocketmq.common.protocol.body.ProducerTableInfo;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

public class ProducerManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    /**
     * channel过期时间
     */
    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;
    /**
     * 获取可用channel的重试 数量
     */
    private static final int GET_AVAILABLE_CHANNEL_RETRY_COUNT = 3;
    /**
     * key 为 生产者名, value.key 为 客户端 channel, value.value 为客户端信息
     */
    private final ConcurrentHashMap<String /* group name */, ConcurrentHashMap<Channel, ClientChannelInfo>> groupChannelTable =
        new ConcurrentHashMap<>();
    /**
     * key 为 客户端 id, value 为 客户端 Channel
     */
    private final ConcurrentHashMap<String, Channel> clientChannelTable = new ConcurrentHashMap<>();
    /**
     * broker状态统计
     */
    protected final BrokerStatsManager brokerStatsManager;
    /**
     * 正数计数器
     */
    private PositiveAtomicCounter positiveAtomicCounter = new PositiveAtomicCounter();
    /**
     * 客户端改变监听器
     */
    private final List<ProducerChangeListener> producerChangeListenerList = new CopyOnWriteArrayList<>();

    public ProducerManager() {
        this.brokerStatsManager = null;
    }

    public ProducerManager(final BrokerStatsManager brokerStatsManager) {
        this.brokerStatsManager = brokerStatsManager;
    }

    public int groupSize() {
        return this.groupChannelTable.size();
    }

    public boolean groupOnline(String group) {
        Map<Channel, ClientChannelInfo> channels = this.groupChannelTable.get(group);
        return channels != null && !channels.isEmpty();
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<Channel, ClientChannelInfo>> getGroupChannelTable() {
        return groupChannelTable;
    }

    /**
     * 将groupChannelTable 根据 group 分组  group =>  List<ProducerInfo> 映射
     * @return
     */
    public ProducerTableInfo getProducerTable() {
        //将groupChannelTable 根据 group 分组  group =>  List<ProducerInfo> 映射
        Map<String, List<ProducerInfo>> map = new HashMap<>();
        //遍历 groupChannelTable 将 客户端信息 根据 group 添加到 map
        for (String group : this.groupChannelTable.keySet()) {
            for (Entry<Channel, ClientChannelInfo> entry: this.groupChannelTable.get(group).entrySet()) {
                ClientChannelInfo clientChannelInfo = entry.getValue();
                if (map.containsKey(group)) {
                    map.get(group).add(new ProducerInfo(
                            clientChannelInfo.getClientId(),
                            clientChannelInfo.getChannel().remoteAddress().toString(),
                            clientChannelInfo.getLanguage(),
                            clientChannelInfo.getVersion(),
                            clientChannelInfo.getLastUpdateTimestamp()
                    ));
                } else {
                    map.put(group, new ArrayList<ProducerInfo>(Collections.singleton(new ProducerInfo(
                            clientChannelInfo.getClientId(),
                            clientChannelInfo.getChannel().remoteAddress().toString(),
                            clientChannelInfo.getLanguage(),
                            clientChannelInfo.getVersion(),
                            clientChannelInfo.getLastUpdateTimestamp()
                    ))));
                }
            }
        }
        return new ProducerTableInfo(map);
    }

    /**
     * 遍历 groupChannelTable 判断客户端是否 超时 超时进行 移除  从groupChannelTable  从 clientChannelTable 移除 关闭 该客户端的连接
     */
    public void scanNotActiveChannel() {

        Iterator<Map.Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>>> iterator = this.groupChannelTable.entrySet().iterator();
        //遍历 groupChannelTable 判断客户端是否 超时 超时进行 移除  从groupChannelTable  从 clientChannelTable 移除 关闭 该客户端的连接
        while (iterator.hasNext()) {
            Map.Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>> entry = iterator.next();

            final String group = entry.getKey();
            final ConcurrentHashMap<Channel, ClientChannelInfo> chlMap = entry.getValue();

            Iterator<Entry<Channel, ClientChannelInfo>> it = chlMap.entrySet().iterator();
            while (it.hasNext()) {
                Entry<Channel, ClientChannelInfo> item = it.next();
                // final Integer id = item.getKey();
                final ClientChannelInfo info = item.getValue();
                //判断客户端是否 超时 超时进行 移除 从groupChannelTable 移除 从 clientChannelTable 移除 关闭 该客户端的连接
                long diff = System.currentTimeMillis() - info.getLastUpdateTimestamp();
                if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                    it.remove();
                    clientChannelTable.remove(info.getClientId());
                    log.warn(
                            "ProducerManager#scanNotActiveChannel: remove expired channel[{}] from ProducerManager groupChannelTable, producer group name: {}",
                            RemotingHelper.parseChannelRemoteAddr(info.getChannel()), group);
                    callProducerChangeListener(ProducerGroupEvent.CLIENT_UNREGISTER, group, info);
                    RemotingUtil.closeChannel(info.getChannel());
                }
            }

            if (chlMap.isEmpty()) {
                log.warn("SCAN: remove expired channel from ProducerManager groupChannelTable, all clear, group={}", group);
                iterator.remove();
                callProducerChangeListener(ProducerGroupEvent.GROUP_UNREGISTER, group, null);
            }
        }
    }

    /**
     * 客户端关闭事件
     * 遍历 groupChannelTable 从groupChannelTable 移除 该客户端信息 从 clientChannelTable 移除 该信息 调用生产者监听器
     * @param remoteAddr
     * @param channel
     * @return
     */
    public synchronized boolean doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        boolean removed = false;
        if (channel != null) {
            //遍历 groupChannelTable 从groupChannelTable 移除 该客户端信息 从 clientChannelTable 移除 该信息
            for (final Map.Entry<String, ConcurrentHashMap<Channel, ClientChannelInfo>> entry : this.groupChannelTable
                    .entrySet()) {
                final String group = entry.getKey();
                final ConcurrentHashMap<Channel, ClientChannelInfo> clientChannelInfoTable =
                        entry.getValue();
                final ClientChannelInfo clientChannelInfo =
                        clientChannelInfoTable.remove(channel);
                if (clientChannelInfo != null) {
                    clientChannelTable.remove(clientChannelInfo.getClientId());
                    removed = true;
                    log.info(
                            "NETTY EVENT: remove channel[{}][{}] from ProducerManager groupChannelTable, producer group: {}",
                            clientChannelInfo.toString(), remoteAddr, group);
                    // 调用生产者监听器 客户端取消注册
                    callProducerChangeListener(ProducerGroupEvent.CLIENT_UNREGISTER, group, clientChannelInfo);
                    //该生产组 ,没有生产者 则移除该生产组
                    if (clientChannelInfoTable.isEmpty()) {
                        // 调用生产者监听器 客户端取消注册
                        ConcurrentHashMap<Channel, ClientChannelInfo> oldGroupTable = this.groupChannelTable.remove(group);
                        if (oldGroupTable != null) {
                            log.info("unregister a producer group[{}] from groupChannelTable", group);
                            callProducerChangeListener(ProducerGroupEvent.GROUP_UNREGISTER, group, null);
                        }
                    }
                }

            }
        }
        return removed;
    }

    /**
     * 根据客户端id 获取客户端信息 存在则 进行 添加 groupChannelTable 添加 clientChannelTable
     * 存在则更新最近更新时间
     * @param group
     * @param clientChannelInfo
     */
    public synchronized void registerProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        ClientChannelInfo clientChannelInfoFound = null;
        //根据生产者组获取 生产者列表 不存在 则初始
        ConcurrentHashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
        if (null == channelTable) {
            channelTable = new ConcurrentHashMap<>();
            this.groupChannelTable.put(group, channelTable);
        }
        //根据客户端id 获取客户端信息 存在则 进行 添加 groupChannelTable 添加 clientChannelTable
        //存在则更新最近更新时间
        clientChannelInfoFound = channelTable.get(clientChannelInfo.getChannel());
        if (null == clientChannelInfoFound) {
            channelTable.put(clientChannelInfo.getChannel(), clientChannelInfo);
            clientChannelTable.put(clientChannelInfo.getClientId(), clientChannelInfo.getChannel());
            log.info("new producer connected, group: {} channel: {}", group,
                    clientChannelInfo.toString());
        }


        if (clientChannelInfoFound != null) {
            clientChannelInfoFound.setLastUpdateTimestamp(System.currentTimeMillis());
        }
    }

    /**
     * 根据生产者组获取 生产者列表 从groupChannelTable 移除 该客户端信息 从 clientChannelTable 移除 该信息 调用生产者监听器
     * @param group
     * @param clientChannelInfo
     */
    public synchronized void unregisterProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        //根据生产者组获取 生产者列表 从groupChannelTable 移除 该客户端信息 从 clientChannelTable 移除 该信息
        //调用生产者监听器
        ConcurrentHashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
        if (null != channelTable && !channelTable.isEmpty()) {
            ClientChannelInfo old = channelTable.remove(clientChannelInfo.getChannel());
            //从groupChannelTable 移除 该客户端信息 调用生产者监听器  客户端取消注册
            clientChannelTable.remove(clientChannelInfo.getClientId());
            if (old != null) {
                log.info("unregister a producer[{}] from groupChannelTable {}", group,
                        clientChannelInfo.toString());
                callProducerChangeListener(ProducerGroupEvent.CLIENT_UNREGISTER, group, clientChannelInfo);
            }
            //该生产组 ,没有生产者 则移除该生产组 调用生产者监听器  生产组取消注册
            if (channelTable.isEmpty()) {
                this.groupChannelTable.remove(group);
                callProducerChangeListener(ProducerGroupEvent.GROUP_UNREGISTER, group, null);
                log.info("unregister a producer group[{}] from groupChannelTable", group);
            }
        }
    }

    /**
     * 从该生产者组 获取可用的Channel
     * @param groupId
     * @return
     */
    public Channel getAvailableChannel(String groupId) {
        if (groupId == null) {
            return null;
        }
        //该 group 生产者  Channel 集合
        List<Channel> channelList;
        //根据 group 获取 该生产组的 生产者列表
        ConcurrentHashMap<Channel, ClientChannelInfo> channelClientChannelInfoHashMap = groupChannelTable.get(groupId);
        if (channelClientChannelInfoHashMap != null) {
            channelList = new ArrayList<>(channelClientChannelInfoHashMap.keySet());
        } else {
            log.warn("Check transaction failed, channel table is empty. groupId={}", groupId);
            return null;
        }

        int size = channelList.size();
        if (0 == size) {
            log.warn("Channel list is empty. groupId={}", groupId);
            return null;
        }

        Channel lastActiveChannel = null;
        //递增 求余 获取 其中一个 生产者 连接
        int index = positiveAtomicCounter.incrementAndGet() % size;
        Channel channel = channelList.get(index);
        int count = 0;
        //如果该生产者连接 不是激活状态或不可写 则 继续从生产者 列表中找
        boolean isOk = channel.isActive() && channel.isWritable();
        while (count++ < GET_AVAILABLE_CHANNEL_RETRY_COUNT) {
            if (isOk) {
                return channel;
            }
            if (channel.isActive()) {
                lastActiveChannel = channel;
            }
            index = (++index) % size;
            channel = channelList.get(index);
            isOk = channel.isActive() && channel.isWritable();
        }

        return lastActiveChannel;
    }

    public Channel findChannel(String clientId) {
        return clientChannelTable.get(clientId);
    }

    /**
     * 生产者发生改变 调用生产者监听器
     * @param event
     * @param group
     * @param clientChannelInfo
     */
    private void callProducerChangeListener(ProducerGroupEvent event, String group,
        ClientChannelInfo clientChannelInfo) {
        for (ProducerChangeListener listener : producerChangeListenerList) {
            try {
                listener.handle(event, group, clientChannelInfo);
            } catch (Throwable t) {
                log.error("err when call producerChangeListener", t);
            }
        }
    }

    public void appendProducerChangeListener(ProducerChangeListener producerChangeListener) {
        producerChangeListenerList.add(producerChangeListener);
    }
}
