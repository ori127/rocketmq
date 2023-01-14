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
package org.apache.rocketmq.namesrv.routeinfo;

import com.google.common.collect.Sets;
import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BrokerAddrInfo;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.protocol.body.BrokerMemberGroup;
import org.apache.rocketmq.common.protocol.header.NotifyMinBrokerIdChangeRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.UnRegisterBrokerRequestHeader;
import org.apache.rocketmq.common.statictopic.TopicQueueMappingInfo;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.body.TopicConfigAndMappingSerializeWrapper;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class RouteInfoManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    private final static long DEFAULT_BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2;
    /**
     * 锁
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    /**
     * key 为  topic ,value.key 为  brokerName, value.value 为 QueueData
     */
    private final Map<String/* topic */, Map<String, QueueData>> topicQueueTable;
    /**
     * key 为  brokerName ,value 为 BrokerData BrokerData 包含 该 broker 节点列表
     */
    private final Map<String/* brokerName */, BrokerData> brokerAddrTable;
    /**
     * key 为 clusterName,value 为 brokerName 集合
     */
    private final Map<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;
    /**
     * key 为 BrokerAddrInfo , value 为 BrokerLiveInfo
     */
    private final Map<BrokerAddrInfo/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;
    private final Map<BrokerAddrInfo/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;
    /**
     * key 为 topic ,value.key 为 brokerName , value.value 为 TopicQueueMappingInfo
     */
    private final Map<String/* topic */, Map<String/*brokerName*/, TopicQueueMappingInfo>> topicQueueMappingInfoTable;
    /**
     * 取消注册服务
     */
    private final BatchUnregistrationService unRegisterService;

    private final NamesrvController namesrvController;
    private final NamesrvConfig namesrvConfig;

    public RouteInfoManager(final NamesrvConfig namesrvConfig, NamesrvController namesrvController) {
        this.topicQueueTable = new ConcurrentHashMap<>(1024);
        this.brokerAddrTable = new ConcurrentHashMap<>(128);
        this.clusterAddrTable = new ConcurrentHashMap<>(32);
        this.brokerLiveTable = new ConcurrentHashMap<>(256);
        this.filterServerTable = new ConcurrentHashMap<>(256);
        this.topicQueueMappingInfoTable = new ConcurrentHashMap<>(1024);
        this.unRegisterService = new BatchUnregistrationService(this, namesrvConfig);
        this.namesrvConfig = namesrvConfig;
        this.namesrvController = namesrvController;
    }

    /**
     * 启动取消注册服务
     */
    public void start() {
        this.unRegisterService.start();
    }

    /**
     * 关闭取消注册服务
     */
    public void shutdown() {
        this.unRegisterService.shutdown(true);
    }

    /**
     * 取消注册
     * @param unRegisterRequest
     * @return
     */
    public boolean submitUnRegisterBrokerRequest(UnRegisterBrokerRequestHeader unRegisterRequest) {
        return this.unRegisterService.submit(unRegisterRequest);
    }

    // For test only
    int blockedUnRegisterRequests() {
        return this.unRegisterService.queueLength();
    }

    /**
     * 获取集群信息
     * @return
     */
    public ClusterInfo getAllClusterInfo() {
        ClusterInfo clusterInfoSerializeWrapper = new ClusterInfo();
        clusterInfoSerializeWrapper.setBrokerAddrTable(this.brokerAddrTable);
        clusterInfoSerializeWrapper.setClusterAddrTable(this.clusterAddrTable);
        return clusterInfoSerializeWrapper;
    }

    /**
     * 注册 topic QueueData
     * @param topic
     * @param queueDatas
     */
    public void registerTopic(final String topic, List<QueueData> queueDatas) {
        if (queueDatas == null || queueDatas.isEmpty()) {
            return;
        }
        try {
            //上锁
            this.lock.writeLock().lockInterruptibly();
            //如果topicQueueTable已经包含 该 topic 不做任何处理
            if (this.topicQueueTable.containsKey(topic)) {
                log.info("Topic route already exist.{}, {}", topic, this.topicQueueTable.get(topic));
            } else {
                // check and construct queue data map
                Map<String, QueueData> queueDataMap = new HashMap<>();
                //遍历 queueData 如果 brokerAddrTable 已经包含 该 broker 则不进行添加
                for (QueueData queueData : queueDatas) {
                    if (!this.brokerAddrTable.containsKey(queueData.getBrokerName())) {
                        log.warn("Register topic contains illegal broker, {}, {}", topic, queueData);
                        return;
                    }
                    //添加到 queueDataMap 映射 brokerName 和  QueueData 关系
                    queueDataMap.put(queueData.getBrokerName(), queueData);
                }
                //映射 topic 和  queueData 信息
                this.topicQueueTable.put(topic, queueDataMap);
                log.info("Register topic route:{}, {}", topic, queueDatas);
            }
        } catch (Exception e) {
            log.error("registerTopic Exception", e);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    /**
     * 删除整个对应的topic
     * @param topic
     */
    public void deleteTopic(final String topic) {
        try {
            this.lock.writeLock().lockInterruptibly();
            this.topicQueueTable.remove(topic);
        } catch (Exception e) {
            log.error("deleteTopic Exception", e);
        } finally {
            this.lock.writeLock().unlock();
        }
    }
    /**
     * 移除topic 下 该集群下 所有 broke 相关的  QueueData
     * @param topic
     */
    public void deleteTopic(final String topic, final String clusterName) {
        try {
            this.lock.writeLock().lockInterruptibly();
            //get all the brokerNames fot the specified cluster
            //根据集群获取所有 brokerNames
            Set<String> brokerNames = this.clusterAddrTable.get(clusterName);
            if (brokerNames == null || brokerNames.isEmpty()) {
                return;
            }
            //get the store information for single topic
            //获取 topic 的 brokerName 和 QueueData 关联关系
            Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
            //移除topic 下 该集群下 所有 broke 相关的  QueueData
            if (queueDataMap != null) {
                //遍历该topic的 QueueData 移除 是该brokerName 的 队列
                for (String brokerName : brokerNames) {
                    final QueueData removedQD = queueDataMap.remove(brokerName);
                    if (removedQD != null) {
                        log.info("deleteTopic, remove one broker's topic {} {} {}", brokerName, topic, removedQD);
                    }
                }
                //如果整个QueueData为空 则移除 topic
                if (queueDataMap.isEmpty()) {
                    log.info("deleteTopic, remove the topic all queue {} {}", clusterName, topic);
                    this.topicQueueTable.remove(topic);
                }
            }
        } catch (Exception e) {
            log.error("deleteTopic Exception", e);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    /**
     * 获取所有 topic
     * @return
     */
    public TopicList getAllTopicList() {
        TopicList topicList = new TopicList();
        try {
            this.lock.readLock().lockInterruptibly();
            topicList.getTopicList().addAll(this.topicQueueTable.keySet());
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }

        return topicList;
    }
    //注册Broker
    public RegisterBrokerResult registerBroker(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final String haServerAddr,
        final String zoneName,
        final Long timeoutMillis,
        final TopicConfigSerializeWrapper topicConfigWrapper,
        final List<String> filterServerList,
        final Channel channel) {
        return registerBroker(clusterName, brokerAddr, brokerName, brokerId, haServerAddr, zoneName, timeoutMillis, false, topicConfigWrapper, filterServerList, channel);
    }

    public RegisterBrokerResult registerBroker(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId,
        final String haServerAddr,
        final String zoneName,
        final Long timeoutMillis,
        final Boolean enableActingMaster,
        final TopicConfigSerializeWrapper topicConfigWrapper,
        final List<String> filterServerList,
        final Channel channel) {
        RegisterBrokerResult result = new RegisterBrokerResult();
        try {
            //上锁
            this.lock.writeLock().lockInterruptibly();

            //init or update the cluster info
            //初始化 集群信息 将该 broker 添加到 该映射下
            Set<String> brokerNames = ConcurrentHashMapUtils.computeIfAbsent((ConcurrentHashMap<String, Set<String>>) this.clusterAddrTable, clusterName, k -> new HashSet<>());
            // 添加 broker
            brokerNames.add(brokerName);

            boolean registerFirst = false;
            //brokerAddrTable根据 broker 获取 BrokerData 如果不存在说 是第一次注册 添加 brokerName => BrokerData 映射
            BrokerData brokerData = this.brokerAddrTable.get(brokerName);
            if (null == brokerData) {
                registerFirst = true;
                brokerData = new BrokerData(clusterName, brokerName, new HashMap<>());
                this.brokerAddrTable.put(brokerName, brokerData);
            }
            //是否 旧版本的 broker 旧版本 不存在 enableActingMaster 属性
            boolean isOldVersionBroker = enableActingMaster == null;
            //是否自动 选主
            brokerData.setEnableActingMaster(!isOldVersionBroker && enableActingMaster);
            //设置 该 broker zone 过滤的时候要用到
            brokerData.setZoneName(zoneName);
            //获取 broker 的 节点集合
            Map<Long, String> brokerAddrsMap = brokerData.getBrokerAddrs();

            boolean isMinBrokerIdChanged = false;
            //记录之前的 broker 节点 中的 最小 id
            long prevMinBrokerId = 0;
            //如果  broker 的 节点集合 存在 获取最小的 broker 节点 最小 id
            if (!brokerAddrsMap.isEmpty()) {
                prevMinBrokerId = Collections.min(brokerAddrsMap.keySet());
            }
            //注册的 broker 的 id 比当前 broker 中 最小 id 要小 则需要改变  最小 id
            if (brokerId < prevMinBrokerId) {
                isMinBrokerIdChanged = true;
            }

            //Switch slave to master: first remove <1, IP:PORT> in namesrv, then add <0, IP:PORT>
            //The same IP:PORT must only have one record in brokerAddrTable
            //FIXME:: 断线重连? 从 broker 节点列表 中 移除 该节点 旧的信息 是该节点 ip 但是 id 确不是 该 broker的 id
            // 如果 该 broker 地址 和 id 都没有发生 改变则不需要移除 如果 id 发生改变 则需要从 移除 重新 进行 添加
            brokerAddrsMap.entrySet().removeIf(item -> null != brokerAddr && brokerAddr.equals(item.getValue()) && brokerId != item.getKey());

            //If Local brokerId stateVersion bigger than the registering one,
            //根据 id 获取 broker 节点列表  该 id 的 broker 地址 也就是 该 id 旧的 地址
            String oldBrokerAddr = brokerAddrsMap.get(brokerId);
            //如果新地址 和 旧 地址不一致
            if (null != oldBrokerAddr && !oldBrokerAddr.equals(brokerAddr)) {
                //从 brokerLiveTable 当中 获取 旧的broker 的 BrokerLiveInfo
                BrokerLiveInfo oldBrokerInfo = brokerLiveTable.get(new BrokerAddrInfo(clusterName, oldBrokerAddr));
                //TODO:: 这边 如果新的 发生改变 为什么不添加 brokerLiveTable 下面版本旧 还为什么要做移除操作
                if (null != oldBrokerInfo) {
                    //获取  旧的broker 的 BrokerLiveInfo 的 版本号
                    long oldStateVersion = oldBrokerInfo.getDataVersion().getStateVersion();
                    long newStateVersion = topicConfigWrapper.getDataVersion().getStateVersion();
                    //旧的版本号 比 新的版本号 大 表示 新的注册 版本 已经比较 就 忽略该注册
                    if (oldStateVersion > newStateVersion) {
                        log.warn("Registered Broker conflicts with the existed one, just ignore.: Cluster:{}, BrokerName:{}, BrokerId:{}, " +
                                "Old BrokerAddr:{}, Old Version:{}, New BrokerAddr:{}, New Version:{}.",
                            clusterName, brokerName, brokerId, oldBrokerAddr, oldStateVersion, brokerAddr, newStateVersion);
                        //Remove the rejected brokerAddr from brokerLiveTable.
                        //移除 新的 注册 broker 信息
                        brokerLiveTable.remove(new BrokerAddrInfo(clusterName, brokerAddr));
                        return result;
                    }
                }
            }
            //如果 brokerAddrsMap 不包含 该 brokerId 并且 topic 配置信息 只有一个 不进行注册 FIXME:: 为什么不注册
            if (!brokerAddrsMap.containsKey(brokerId) && topicConfigWrapper.getTopicConfigTable().size() == 1) {
                log.warn("Can't register topicConfigWrapper={} because broker[{}]={} has not registered.",
                    topicConfigWrapper.getTopicConfigTable(), brokerId, brokerAddr);
                return null;
            }
            //形成 brokerAddr 映射
            //根据 brokerId 映射成 新的brokerAddr
            String oldAddr = brokerAddrsMap.put(brokerId, brokerAddr);
            //第一次注册
            registerFirst = registerFirst || (StringUtils.isEmpty(oldAddr));
            //是否是主 broker
            boolean isMaster = MixAll.MASTER_ID == brokerId;
            //是否是 主被  条件 如下 不是 主 broker 且 该 brokerId 是 新的 broker 节点 当中 最小的 id
            boolean isPrimeSlave = !isOldVersionBroker && !isMaster
                && brokerId == Collections.min(brokerAddrsMap.keySet());
            //如果是 主 broker 或者 是 主要的被
            //主要的被 则去掉 可写 权限 进行 配置更新
            //如果是 主 broker 进行 配置更新
            if (null != topicConfigWrapper && (isMaster || isPrimeSlave)) {

                ConcurrentMap<String, TopicConfig> tcTable =
                    topicConfigWrapper.getTopicConfigTable();
                if (tcTable != null) {
                    //遍历 topic 配置
                    for (Map.Entry<String, TopicConfig> entry : tcTable.entrySet()) {
                        //第一次注册 或  topic 配置是否已经发生 改变
                        if (registerFirst || this.isTopicConfigChanged(clusterName, brokerAddr,
                            topicConfigWrapper.getDataVersion(), brokerName,
                            entry.getValue().getTopicName())) {
                            final TopicConfig topicConfig = entry.getValue();
                            //做为主要的被 去掉可写
                            if (isPrimeSlave) {
                                // Wipe write perm for prime slave
                                topicConfig.setPerm(topicConfig.getPerm() & (~PermName.PERM_WRITE));
                            }
                            //更新 topic 下的 QueueDate 形成 topicQueueTable 映射
                            this.createAndUpdateQueueData(brokerName, topicConfig);
                        }
                    }
                }
                //配置发生改变 或  第一次 注册
                if (this.isBrokerTopicConfigChanged(clusterName, brokerAddr, topicConfigWrapper.getDataVersion()) || registerFirst) {
                    //topicConfigWrapper 转换成 TopicConfigAndMappingSerializeWrapper TODO:: topicConfigWrapper = TopicConfigAndMappingSerializeWrapper TopicQueueMappingInfo 是什么信息
                    TopicConfigAndMappingSerializeWrapper mappingSerializeWrapper = TopicConfigAndMappingSerializeWrapper.from(topicConfigWrapper);
                    Map<String, TopicQueueMappingInfo> topicQueueMappingInfoMap = mappingSerializeWrapper.getTopicQueueMappingInfoMap();
                    //the topicQueueMappingInfoMap should never be null, but can be empty
                    //遍历 topicQueueMappingInfoMap 添加到  topicQueueMappingInfoTable topicQueueMappingInfoMap
                    for (Map.Entry<String, TopicQueueMappingInfo> entry : topicQueueMappingInfoMap.entrySet()) {
                        if (!topicQueueMappingInfoTable.containsKey(entry.getKey())) {
                            topicQueueMappingInfoTable.put(entry.getKey(), new HashMap<>());
                        }
                        //Note asset brokerName equal entry.getValue().getBname()
                        //here use the mappingDetail.bname
                        topicQueueMappingInfoTable.get(entry.getKey()).put(entry.getValue().getBname(), entry.getValue());
                    }
                }
            }

            BrokerAddrInfo brokerAddrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
            //生成 brokerAddrInfo 和  BrokerLiveInfo 映射关系
            BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(brokerAddrInfo,
                new BrokerLiveInfo(
                    System.currentTimeMillis(),
                    timeoutMillis == null ? DEFAULT_BROKER_CHANNEL_EXPIRED_TIME : timeoutMillis,
                    topicConfigWrapper == null ? new DataVersion() : topicConfigWrapper.getDataVersion(),
                    channel,
                    haServerAddr));
            if (null == prevBrokerLiveInfo) {
                log.info("new broker registered, {} HAService: {}", brokerAddrInfo, haServerAddr);
            }
            //过滤服务为空  移除 该 broker 过滤服务
            //否则添加 该 broker 过滤服务
            if (filterServerList != null) {
                if (filterServerList.isEmpty()) {
                    this.filterServerTable.remove(brokerAddrInfo);
                } else {
                    this.filterServerTable.put(brokerAddrInfo, filterServerList);
                }
            }
            //如果该broker 不是 主 则 从 broker 节点列表中 获取 master 结果设置成 主broker 地址
            if (MixAll.MASTER_ID != brokerId) {
                String masterAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                if (masterAddr != null) {
                    BrokerAddrInfo masterAddrInfo = new BrokerAddrInfo(clusterName, masterAddr);
                    BrokerLiveInfo masterLiveInfo = this.brokerLiveTable.get(masterAddrInfo);
                    if (masterLiveInfo != null) {
                        result.setHaServerAddr(masterLiveInfo.getHaServerAddr());
                        result.setMasterAddr(masterAddr);
                    }
                }
            }
            //最小id发生改变 调用 通知最小id发生改变 通知 broker 下面 除了 最小 id 的 broker 节点 所有节点
            if (isMinBrokerIdChanged && namesrvConfig.isNotifyMinBrokerIdChanged()) {
                notifyMinBrokerIdChanged(brokerAddrsMap, null,
                    this.brokerLiveTable.get(brokerAddrInfo).getHaServerAddr());
            }
        } catch (Exception e) {
            log.error("registerBroker Exception", e);
        } finally {
            this.lock.writeLock().unlock();
        }

        return result;
    }

    /**
     * 获取集群下 的 broker 下的 broker 节点列表
     * @param clusterName
     * @param brokerName
     * @return
     */
    public BrokerMemberGroup getBrokerMemberGroup(String clusterName, String brokerName) {
        BrokerMemberGroup groupMember = new BrokerMemberGroup(clusterName, brokerName);
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                final BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (brokerData != null) {
                    groupMember.getBrokerAddrs().putAll(brokerData.getBrokerAddrs());
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("Get broker member group exception", e);
        }
        return groupMember;
    }

    /**
     * 根据集群 和 broker地址 先判断 clusterName 和 brokerAddr 是否相同 再 判断版本是否相同
     * 判断版本是否发生改变
     * @param clusterName
     * @param brokerAddr
     * @param dataVersion
     * @return
     */
    public boolean isBrokerTopicConfigChanged(final String clusterName, final String brokerAddr,
        final DataVersion dataVersion) {
        //根据集群 和 broker地址 获取 版本号 先判断 clusterName 和 brokerAddr 是否相同 再 判断版本是否相同
        DataVersion prev = queryBrokerTopicConfig(clusterName, brokerAddr);
        return null == prev || !prev.equals(dataVersion);
    }

    /**
     * topic 配置是否已经发生 改变
     * 根据集群 和 broker地址 获取 版本号 判断版本是否发生改变
     * broker 在该 topic broker节点内
     * @param clusterName
     * @param brokerAddr
     * @param dataVersion
     * @param brokerName
     * @param topic
     * @return
     */
    public boolean isTopicConfigChanged(final String clusterName, final String brokerAddr,
        final DataVersion dataVersion, String brokerName, String topic) {
        //根据集群 和 broker地址 获取  判断 clusterName 和 brokerAddr 是否相同 再 版本号 判断版本是否发生改变
        boolean isChange = isBrokerTopicConfigChanged(clusterName, brokerAddr, dataVersion);
        if (isChange) {
            return true;
        }
        //根据topic 获取 topic 的 broker  QueueData 信息
        final Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
        if (queueDataMap == null || queueDataMap.isEmpty()) {
            return true;
        }
        //如果 topicQueueTable 不包含 该 broker 说明发生 改变
        // The topicQueueTable already contains the broker
        return !queueDataMap.containsKey(brokerName);
    }

    /**
     * 根据集群 和 broker地址 获取 版本号
     * @param clusterName
     * @param brokerAddr
     * @return
     */
    public DataVersion queryBrokerTopicConfig(final String clusterName, final String brokerAddr) {
        //根据集群 和 broker地址 获取 版本号
        BrokerAddrInfo addrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
        BrokerLiveInfo prev = this.brokerLiveTable.get(addrInfo);
        if (prev != null) {
            return prev.getDataVersion();
        }
        return null;
    }

    /**
     * 更新 broker 最新修改时间
     * @param clusterName
     * @param brokerAddr
     */
    public void updateBrokerInfoUpdateTimestamp(final String clusterName, final String brokerAddr) {
        BrokerAddrInfo addrInfo = new BrokerAddrInfo(clusterName, brokerAddr);
        BrokerLiveInfo prev = this.brokerLiveTable.get(addrInfo);
        if (prev != null) {
            prev.setLastUpdateTimestamp(System.currentTimeMillis());
        }
    }

    /**
     * 创建 或 更新 topic 下的  QueueData
     * @param brokerName
     * @param topicConfig
     */
    private void createAndUpdateQueueData(final String brokerName, final TopicConfig topicConfig) {
        //生成 QueueData
        QueueData queueData = new QueueData();
        queueData.setBrokerName(brokerName);
        queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
        queueData.setReadQueueNums(topicConfig.getReadQueueNums());
        queueData.setPerm(topicConfig.getPerm());
        queueData.setTopicSysFlag(topicConfig.getTopicSysFlag());
        //获取 该 topic 下 队列 信息
        Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topicConfig.getTopicName());
        //不存在 queueDataMap 则 直接添加 broker 和 queueData 关系
        if (null == queueDataMap) {
            queueDataMap = new HashMap<>();
            queueDataMap.put(brokerName, queueData);
            this.topicQueueTable.put(topicConfig.getTopicName(), queueDataMap);
            log.info("new topic registered, {} {}", topicConfig.getTopicName(), queueData);
        } else {
            //存在 queueDataMap 则 根据 brokerName 获取 QueueData 若果 不存在 则设置  broker 和 queueData 关系
            final QueueData existedQD = queueDataMap.get(brokerName);
            if (existedQD == null) {
                queueDataMap.put(brokerName, queueData);
            } else if (!existedQD.equals(queueData)) {
                //如果 存在  broker 和 queueData 关系 则更新  queueData
                log.info("topic changed, {} OLD: {} NEW: {}", topicConfig.getTopicName(), existedQD,
                    queueData);
                queueDataMap.put(brokerName, queueData);
            }
        }
    }

    public int wipeWritePermOfBrokerByLock(final String brokerName) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                return operateWritePermOfBroker(brokerName, RequestCode.WIPE_WRITE_PERM_OF_BROKER);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("wipeWritePermOfBrokerByLock Exception", e);
        }

        return 0;
    }

    /**
     * 给该 broker 下的 Queue添加 读写权限
     * @param brokerName
     * @return
     */
    public int addWritePermOfBrokerByLock(final String brokerName) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                return operateWritePermOfBroker(brokerName, RequestCode.ADD_WRITE_PERM_OF_BROKER);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("wipeWritePermOfBrokerByLock Exception", e);
        }
        return 0;
    }

    private int operateWritePermOfBroker(final String brokerName, final int requestCode) {
        int topicCnt = 0;
        //遍历所有 topic 获取 该 broker 下的 QueueData 根据 code 修改 QueueData 的读写 权限
        for (Entry<String, Map<String, QueueData>> entry : this.topicQueueTable.entrySet()) {
            Map<String, QueueData> qdMap = entry.getValue();

            final QueueData qd = qdMap.get(brokerName);
            if (qd == null) {
                continue;
            }
            int perm = qd.getPerm();
            switch (requestCode) {
                case RequestCode.WIPE_WRITE_PERM_OF_BROKER:
                    perm &= ~PermName.PERM_WRITE;
                    break;
                case RequestCode.ADD_WRITE_PERM_OF_BROKER:
                    perm = PermName.PERM_READ | PermName.PERM_WRITE;
                    break;
            }
            qd.setPerm(perm);
            topicCnt++;
        }
        return topicCnt;
    }
    //取消注册Broker
    public void unregisterBroker(
        final String clusterName,
        final String brokerAddr,
        final String brokerName,
        final long brokerId) {
        UnRegisterBrokerRequestHeader unRegisterBrokerRequest = new UnRegisterBrokerRequestHeader();
        unRegisterBrokerRequest.setClusterName(clusterName);
        unRegisterBrokerRequest.setBrokerAddr(brokerAddr);
        unRegisterBrokerRequest.setBrokerName(brokerName);
        unRegisterBrokerRequest.setBrokerId(brokerId);

        unRegisterBroker(Sets.newHashSet(unRegisterBrokerRequest));
    }

    public void unRegisterBroker(Set<UnRegisterBrokerRequestHeader> unRegisterRequests) {
        try {
            Set<String> removedBroker = new HashSet<>();
            Set<String> reducedBroker = new HashSet<>();
            Map<String, BrokerStatusChangeInfo> needNotifyBrokerMap = new HashMap<>();
            //上锁
            this.lock.writeLock().lockInterruptibly();
            for (final UnRegisterBrokerRequestHeader unRegisterRequest : unRegisterRequests) {
                final String brokerName = unRegisterRequest.getBrokerName();
                final String clusterName = unRegisterRequest.getClusterName();
                //根据 集群名称 和 brokerName 构建 BrokerAddrInfo
                BrokerAddrInfo brokerAddrInfo = new BrokerAddrInfo(clusterName, unRegisterRequest.getBrokerAddr());
                //移除 该 BrokerAddrInfo 的 BrokerLiveInfo 映射
                BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.remove(brokerAddrInfo);
                log.info("unregisterBroker, remove from brokerLiveTable {}, {}",
                    brokerLiveInfo != null ? "OK" : "Failed",
                    brokerAddrInfo
                );
                //移除 该 BrokerAddrInfo 的  Filter Server 映射
                this.filterServerTable.remove(brokerAddrInfo);

                boolean removeBrokerName = false;
                boolean isMinBrokerIdChanged = false;
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (null != brokerData) {
                    //判断被移除的 broker 的 是不是 该 broker 节点 列表下的 最小Id
                    if (!brokerData.getBrokerAddrs().isEmpty() &&
                        unRegisterRequest.getBrokerId().equals(Collections.min(brokerData.getBrokerAddrs().keySet()))) {
                        isMinBrokerIdChanged = true;
                    }
                    //从 该 broker 节点列表 下 BrokerData 移除 该 broker
                    String addr = brokerData.getBrokerAddrs().remove(unRegisterRequest.getBrokerId());
                    log.info("unregisterBroker, remove addr from brokerAddrTable {}, {}",
                        addr != null ? "OK" : "Failed",
                        brokerAddrInfo
                    );
                    //如果 删除之后 该 broker 节点 列表下 全部 为空 则 删除 该 BrokerData 映射
                    if (brokerData.getBrokerAddrs().isEmpty()) {
                        this.brokerAddrTable.remove(brokerName);
                        log.info("unregisterBroker, remove name from brokerAddrTable OK, {}",
                            brokerName
                        );
                        //整个broker 被移除 标记
                        removeBrokerName = true;
                    } else if (isMinBrokerIdChanged) {
                        //如果最小id 发生了改变 则添加到 needNotifyBrokerMap
                        needNotifyBrokerMap.put(brokerName, new BrokerStatusChangeInfo(
                            brokerData.getBrokerAddrs(), addr, null));
                    }
                }
                //如果整个 brokerData 被移除 则 从 clusterAddrTable 获取 该集群  broker 集合 移除 该集群下 的  该broker 的映射
                //如果该集群的 broker 集合找整个 为空 则从 clusterAddrTable 移除 该集群
                if (removeBrokerName) {
                    Set<String> nameSet = this.clusterAddrTable.get(clusterName);
                    if (nameSet != null) {
                        boolean removed = nameSet.remove(brokerName);
                        log.info("unregisterBroker, remove name from clusterAddrTable {}, {}",
                            removed ? "OK" : "Failed",
                            brokerName);

                        if (nameSet.isEmpty()) {
                            this.clusterAddrTable.remove(clusterName);
                            log.info("unregisterBroker, remove cluster from clusterAddrTable {}",
                                clusterName
                            );
                        }
                    }
                    //记录移除的 broker
                    removedBroker.add(brokerName);
                } else {
                    //记录减少的 broker
                    reducedBroker.add(brokerName);
                }
            }
            //清除 topic
            cleanTopicByUnRegisterRequests(removedBroker, reducedBroker);

            if (!needNotifyBrokerMap.isEmpty() && namesrvConfig.isNotifyMinBrokerIdChanged()) {
                notifyMinBrokerIdChanged(needNotifyBrokerMap);
            }
        } catch (Exception e) {
            log.error("unregisterBroker Exception", e);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    private void cleanTopicByUnRegisterRequests(Set<String> removedBroker, Set<String> reducedBroker) {
        Iterator<Entry<String, Map<String, QueueData>>> itMap = this.topicQueueTable.entrySet().iterator();
        //遍历 topicQueueTable 移除 topic 下的 移除 removedBroker 相关的 QueueData 如果 该 queueDataMap 为空 则整个 topic 进行移除
        //遍历 topicQueueTable
        while (itMap.hasNext()) {
            Entry<String, Map<String, QueueData>> entry = itMap.next();

            String topic = entry.getKey();
            Map<String, QueueData> queueDataMap = entry.getValue();

            for (final String brokerName : removedBroker) {
                final QueueData removedQD = queueDataMap.remove(brokerName);
                if (removedQD != null) {
                    log.debug("removeTopicByBrokerName, remove one broker's topic {} {}", topic, removedQD);
                }
            }

            if (queueDataMap.isEmpty()) {
                log.debug("removeTopicByBrokerName, remove the topic all queue {}", topic);
                itMap.remove();
            }
            //broker 减少 如果该 broker 节点当中 没有 主节点 则 取消 写权限
            for (final String brokerName : reducedBroker) {
                final QueueData queueData = queueDataMap.get(brokerName);
                if (queueData != null) {
                    if (this.brokerAddrTable.get(brokerName).isEnableActingMaster()) {
                        // Master has been unregistered, wipe the write perm
                        if (isNoMasterExists(brokerName)) {
                            queueData.setPerm(queueData.getPerm() & (~PermName.PERM_WRITE));
                        }
                    }
                }
            }
        }
    }

    /**
     * 判断 broker 是否 存在 Master
     * @param brokerName
     * @return
     */
    private boolean isNoMasterExists(String brokerName) {
        final BrokerData brokerData = this.brokerAddrTable.get(brokerName);
        if (brokerData == null) {
            return true;
        }

        if (brokerData.getBrokerAddrs().size() == 0) {
            return true;
        }

        return Collections.min(brokerData.getBrokerAddrs().keySet()) > 0;
    }

    public TopicRouteData pickupTopicRouteData(final String topic) {
        TopicRouteData topicRouteData = new TopicRouteData();
        boolean foundQueueData = false;
        boolean foundBrokerData = false;
        //topic 相关  BrokerData
        List<BrokerData> brokerDataList = new LinkedList<>();
        topicRouteData.setBrokerDatas(brokerDataList);

        HashMap<String, List<String>> filterServerMap = new HashMap<>();
        topicRouteData.setFilterServerTable(filterServerMap);

        try {
            this.lock.readLock().lockInterruptibly();
            //根据topic 获取  QueueData
            Map<String, QueueData> queueDataMap = this.topicQueueTable.get(topic);
            if (queueDataMap != null) {
                //设置 QueueData
                topicRouteData.setQueueDatas(new ArrayList<>(queueDataMap.values()));
                foundQueueData = true;

                Set<String> brokerNameSet = new HashSet<>(queueDataMap.keySet());
                //遍历 该 topic 相关 brokerName 集合 获取 BrokerData
                for (String brokerName : brokerNameSet) {
                    BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                    if (null == brokerData) {
                        continue;
                    }
                    //BrokerData clone
                    BrokerData brokerDataClone = new BrokerData(brokerData.getCluster(),
                        brokerData.getBrokerName(),
                        (HashMap<Long, String>) brokerData.getBrokerAddrs().clone(),
                        brokerData.isEnableActingMaster(), brokerData.getZoneName());
                    //topic 相关  BrokerData
                    brokerDataList.add(brokerDataClone);
                    foundBrokerData = true;
                    if (filterServerTable.isEmpty()) {
                        continue;
                    }
                    //topic 相关  BrokerData 关联的 filterServer
                    for (final String brokerAddr : brokerDataClone.getBrokerAddrs().values()) {
                        BrokerAddrInfo brokerAddrInfo = new BrokerAddrInfo(brokerDataClone.getCluster(), brokerAddr);
                        List<String> filterServerList = this.filterServerTable.get(brokerAddrInfo);
                        filterServerMap.put(brokerAddr, filterServerList);
                    }

                }
            }
        } catch (Exception e) {
            log.error("pickupTopicRouteData Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }

        log.debug("pickupTopicRouteData {} {}", topic, topicRouteData);

        if (foundBrokerData && foundQueueData) {

            if (topicRouteData == null) {
                return null;
            }
            //设置 topic 关联的 broker 和 TopicQueueMappingInfo 信息
            topicRouteData.setTopicQueueMappingByBroker(this.topicQueueMappingInfoTable.get(topic));

            if (!namesrvConfig.isSupportActingMaster()) {
                return topicRouteData;
            }
            //如果 topic以 rmq_sys_SYNC_BROKER_MEMBER_ 开头
            if (topic.startsWith(TopicValidator.SYNC_BROKER_MEMBER_GROUP_PREFIX)) {
                return topicRouteData;
            }

            if (topicRouteData.getBrokerDatas().size() == 0 || topicRouteData.getQueueDatas().size() == 0) {
                return topicRouteData;
            }

            boolean needActingMaster = false;
            //遍历 topic 当中 brokerData 判断 是否有 主 broker  是否需要 重新选主的 broker
            for (final BrokerData brokerData : topicRouteData.getBrokerDatas()) {
                if (brokerData.getBrokerAddrs().size() != 0
                    && !brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID)) {
                    needActingMaster = true;
                    break;
                }
            }

            if (!needActingMaster) {
                return topicRouteData;
            }
            //遍历 topic 下的 broker 节点 列表
            for (final BrokerData brokerData : topicRouteData.getBrokerDatas()) {
                final HashMap<Long, String> brokerAddrs = brokerData.getBrokerAddrs();
                //如果 有 master 或者 不自动选主 则进行跳过
                if (brokerAddrs.size() == 0 || brokerAddrs.containsKey(MixAll.MASTER_ID) || !brokerData.isEnableActingMaster()) {
                    continue;
                }

                // No master
                //没有 master 则遍历 queueData 找到没有 master 的 queueData
                // 判断 queueData 是否 具有写权限
                // 没有写 权限 则 找出 最小 id broker 将 该broker id 改成 MASTER_ID
                for (final QueueData queueData : topicRouteData.getQueueDatas()) {
                    if (queueData.getBrokerName().equals(brokerData.getBrokerName())) {
                        if (!PermName.isWriteable(queueData.getPerm())) {
                            final Long minBrokerId = Collections.min(brokerAddrs.keySet());
                            final String actingMasterAddr = brokerAddrs.remove(minBrokerId);
                            brokerAddrs.put(MixAll.MASTER_ID, actingMasterAddr);
                        }
                        break;
                    }
                }

            }

            return topicRouteData;
        }

        return null;
    }

    /**
     * 扫描 Broker 列表长时间没有收到心跳消息 取消注册 关闭 channel
     */
    public void scanNotActiveBroker() {
        try {
            log.info("start scanNotActiveBroker");
            //遍历 brokerLiveTable 检查 该 broker 最近更新时间 + 心跳超时时间 是否超过当前 时间  也就就是长时间没有收到心跳消息 取消注册 关闭 channel
            for (Entry<BrokerAddrInfo, BrokerLiveInfo> next : this.brokerLiveTable.entrySet()) {
                long last = next.getValue().getLastUpdateTimestamp();
                long timeoutMillis = next.getValue().getHeartbeatTimeoutMillis();
                if ((last + timeoutMillis) < System.currentTimeMillis()) {
                    //关闭channel
                    RemotingUtil.closeChannel(next.getValue().getChannel());
                    log.warn("The broker channel expired, {} {}ms", next.getKey(), timeoutMillis);
                    //取消注册
                    this.onChannelDestroy(next.getKey());
                }
            }
        } catch (Exception e) {
            log.error("scanNotActiveBroker exception", e);
        }
    }

    /**
     * 根据 brokerAddrInfo 取消注册
     * @param brokerAddrInfo
     */
    public void onChannelDestroy(BrokerAddrInfo brokerAddrInfo) {
        UnRegisterBrokerRequestHeader unRegisterRequest = new UnRegisterBrokerRequestHeader();
        boolean needUnRegister = false;
        if (brokerAddrInfo != null) {
            try {
                try {
                    this.lock.readLock().lockInterruptibly();
                    needUnRegister = setupUnRegisterRequest(unRegisterRequest, brokerAddrInfo);
                } finally {
                    this.lock.readLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }

        if (needUnRegister) {
            boolean result = this.submitUnRegisterBrokerRequest(unRegisterRequest);
            log.info("the broker's channel destroyed, submit the unregister request at once, " +
                "broker info: {}, submit result: {}", unRegisterRequest, result);
        }
    }

    /**
     * 根据 channel 取消 注册
     * @param channel
     */
    public void onChannelDestroy(Channel channel) {
        //构建 取消注册 请求
        UnRegisterBrokerRequestHeader unRegisterRequest = new UnRegisterBrokerRequestHeader();
        BrokerAddrInfo brokerAddrFound = null;
        boolean needUnRegister = false;
        if (channel != null) {
            try {
                try {
                    this.lock.readLock().lockInterruptibly();
                    //找到channel 对应的 BrokerAddress
                    for (Entry<BrokerAddrInfo, BrokerLiveInfo> entry : this.brokerLiveTable.entrySet()) {
                        if (entry.getValue().getChannel() == channel) {
                            brokerAddrFound = entry.getKey();
                            break;
                        }
                    }

                    if (brokerAddrFound != null) {
                        needUnRegister = setupUnRegisterRequest(unRegisterRequest, brokerAddrFound);
                    }
                } finally {
                    this.lock.readLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }
        //取消注册
        if (needUnRegister) {
            boolean result = this.submitUnRegisterBrokerRequest(unRegisterRequest);
            log.info("the broker's channel destroyed, submit the unregister request at once, " +
                "broker info: {}, submit result: {}", unRegisterRequest, result);
        }
    }

    /**
     * 设置取消注册请求
     * @param unRegisterRequest
     * @param brokerAddrInfo
     * @return
     */
    private boolean setupUnRegisterRequest(UnRegisterBrokerRequestHeader unRegisterRequest,
        BrokerAddrInfo brokerAddrInfo) {
        //构建 取消注册 请求
        unRegisterRequest.setClusterName(brokerAddrInfo.getClusterName());
        unRegisterRequest.setBrokerAddr(brokerAddrInfo.getBrokerAddr());
        //遍历 brokerAddrTable 找到 该集群下 的 brokerData 该 broker地址 所 对应的 brokerName 和 brokerId
        for (Entry<String, BrokerData> stringBrokerDataEntry : this.brokerAddrTable.entrySet()) {
            BrokerData brokerData = stringBrokerDataEntry.getValue();
            if (!brokerAddrInfo.getClusterName().equals(brokerData.getCluster())) {
                continue;
            }

            for (Entry<Long, String> entry : brokerData.getBrokerAddrs().entrySet()) {
                Long brokerId = entry.getKey();
                String brokerAddr = entry.getValue();
                if (brokerAddr.equals(brokerAddrInfo.getBrokerAddr())) {
                    unRegisterRequest.setBrokerName(brokerData.getBrokerName());
                    unRegisterRequest.setBrokerId(brokerId);
                    return true;
                }
            }
        }

        return false;
    }

    private void notifyMinBrokerIdChanged(Map<String, BrokerStatusChangeInfo> needNotifyBrokerMap)
        throws InterruptedException, RemotingConnectException, RemotingTimeoutException, RemotingSendRequestException,
        RemotingTooMuchRequestException {
        //遍历 broker 最小 id 的 broker
        for (String brokerName : needNotifyBrokerMap.keySet()) {
            BrokerStatusChangeInfo brokerStatusChangeInfo = needNotifyBrokerMap.get(brokerName);
            //如果还存在对应 brokerData 则进行通知
            BrokerData brokerData = brokerAddrTable.get(brokerName);
            if (brokerData != null && brokerData.isEnableActingMaster()) {
                notifyMinBrokerIdChanged(brokerStatusChangeInfo.getBrokerAddrs(),
                    brokerStatusChangeInfo.getOfflineBrokerAddr(), brokerStatusChangeInfo.getHaBrokerAddr());
            }
        }
    }

    /**
     *  通知 broker 下面 除了 最小 id 的 broker 节点 所有节点
     * @param brokerAddrMap
     * @param offlineBrokerAddr
     * @param haBrokerAddr
     * @throws InterruptedException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingConnectException
     */
    private void notifyMinBrokerIdChanged(Map<Long, String> brokerAddrMap, String offlineBrokerAddr,
        String haBrokerAddr)
        throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException,
        RemotingTooMuchRequestException, RemotingConnectException {
        if (brokerAddrMap == null || brokerAddrMap.isEmpty() || this.namesrvController == null) {
            return;
        }

        NotifyMinBrokerIdChangeRequestHeader requestHeader = new NotifyMinBrokerIdChangeRequestHeader();
        long minBrokerId = Collections.min(brokerAddrMap.keySet());
        requestHeader.setMinBrokerId(minBrokerId);
        requestHeader.setMinBrokerAddr(brokerAddrMap.get(minBrokerId));
        requestHeader.setOfflineBrokerAddr(offlineBrokerAddr);
        requestHeader.setHaBrokerAddr(haBrokerAddr);
       // 遍历 broker 节点列表 去掉最小的 brokerId 那些节点
        List<String> brokerAddrsNotify = chooseBrokerAddrsToNotify(brokerAddrMap, offlineBrokerAddr);
        log.info("min broker id changed to {}, notify {}, offline broker addr {}", minBrokerId, brokerAddrsNotify, offlineBrokerAddr);
        RemotingCommand request =
            RemotingCommand.createRequestCommand(RequestCode.NOTIFY_MIN_BROKER_ID_CHANGE, requestHeader);
        //进行通知最小 broker 发生改变
        for (String brokerAddr : brokerAddrsNotify) {
            this.namesrvController.getRemotingClient().invokeOneway(brokerAddr, request, 300);
        }
    }

    /**
     * 遍历 broker 节点列表 去掉最小的 brokerId 那些节点
     * @param brokerAddrMap
     * @param offlineBrokerAddr
     * @return
     */
    private List<String> chooseBrokerAddrsToNotify(Map<Long, String> brokerAddrMap, String offlineBrokerAddr) {
        if (offlineBrokerAddr != null || brokerAddrMap.size() == 1) {
            // notify the reset brokers.
            return new ArrayList<>(brokerAddrMap.values());
        }

        // new broker registered, notify previous brokers.
        //遍历 broker节点 不是 最小id
        long minBrokerId = Collections.min(brokerAddrMap.keySet());
        List<String> brokerAddrList = new ArrayList<>();
        for (Long brokerId : brokerAddrMap.keySet()) {
            if (brokerId != minBrokerId) {
                brokerAddrList.add(brokerAddrMap.get(brokerId));
            }
        }
        return brokerAddrList;
    }

    // For test only
    public void printAllPeriodically() {
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                log.info("--------------------------------------------------------");
                {
                    log.info("topicQueueTable SIZE: {}", this.topicQueueTable.size());
                    for (Entry<String, Map<String, QueueData>> next : this.topicQueueTable.entrySet()) {
                        log.info("topicQueueTable Topic: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerAddrTable SIZE: {}", this.brokerAddrTable.size());
                    for (Entry<String, BrokerData> next : this.brokerAddrTable.entrySet()) {
                        log.info("brokerAddrTable brokerName: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerLiveTable SIZE: {}", this.brokerLiveTable.size());
                    for (Entry<BrokerAddrInfo, BrokerLiveInfo> next : this.brokerLiveTable.entrySet()) {
                        log.info("brokerLiveTable brokerAddr: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("clusterAddrTable SIZE: {}", this.clusterAddrTable.size());
                    for (Entry<String, Set<String>> next : this.clusterAddrTable.entrySet()) {
                        log.info("clusterAddrTable clusterName: {} {}", next.getKey(), next.getValue());
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("printAllPeriodically Exception", e);
        }
    }

    public TopicList getSystemTopicList() {
        TopicList topicList = new TopicList();
        try {
            this.lock.readLock().lockInterruptibly();
            //将该 clusterName 和 该集群下 brokerName 添加到 topicList
            for (Map.Entry<String, Set<String>> entry : clusterAddrTable.entrySet()) {
                topicList.getTopicList().add(entry.getKey());
                topicList.getTopicList().addAll(entry.getValue());
            }
            //设置
            if (!brokerAddrTable.isEmpty()) {
                for (String s : brokerAddrTable.keySet()) {
                    BrokerData bd = brokerAddrTable.get(s);
                    HashMap<Long, String> brokerAddrs = bd.getBrokerAddrs();
                    if (brokerAddrs != null && !brokerAddrs.isEmpty()) {
                        Iterator<Long> it2 = brokerAddrs.keySet().iterator();
                        topicList.setBrokerAddr(brokerAddrs.get(it2.next()));
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("getSystemTopicList Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }

        return topicList;
    }

    /**
     *  获取cluster 的 topic 列表
     */
    public TopicList getTopicsByCluster(String cluster) {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                //获取cluster brokerName 集合
                Set<String> brokerNameSet = this.clusterAddrTable.get(cluster);
                for (String brokerName : brokerNameSet) {
                    //遍历 topic 列表 找到 该集群下 broker 的 topic
                    Iterator<Entry<String, Map<String, QueueData>>> topicTableIt =
                        this.topicQueueTable.entrySet().iterator();
                    while (topicTableIt.hasNext()) {
                        Entry<String, Map<String, QueueData>> topicEntry = topicTableIt.next();
                        String topic = topicEntry.getKey();
                        Map<String, QueueData> queueDataMap = topicEntry.getValue();
                        final QueueData qd = queueDataMap.get(brokerName);
                        if (qd != null) {
                            topicList.getTopicList().add(topic);
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getTopicsByCluster Exception", e);
        }

        return topicList;
    }

    public TopicList getUnitTopics() {
        TopicList topicList = new TopicList();
        try {
            this.lock.readLock().lockInterruptibly();
            for (Entry<String, Map<String, QueueData>> topicEntry : this.topicQueueTable.entrySet()) {
                String topic = topicEntry.getKey();
                Map<String, QueueData> queueDatas = topicEntry.getValue();
                if (queueDatas != null && queueDatas.size() > 0
                    && TopicSysFlag.hasUnitFlag(queueDatas.values().iterator().next().getTopicSysFlag())) {
                    topicList.getTopicList().add(topic);
                }
            }
        } catch (Exception e) {
            log.error("getUnitTopics Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }

        return topicList;
    }

    public TopicList getHasUnitSubTopicList() {
        TopicList topicList = new TopicList();
        try {
            this.lock.readLock().lockInterruptibly();
            for (Entry<String, Map<String, QueueData>> topicEntry : this.topicQueueTable.entrySet()) {
                String topic = topicEntry.getKey();
                Map<String, QueueData> queueDatas = topicEntry.getValue();
                if (queueDatas != null && queueDatas.size() > 0
                    && TopicSysFlag.hasUnitSubFlag(queueDatas.values().iterator().next().getTopicSysFlag())) {
                    topicList.getTopicList().add(topic);
                }
            }
        } catch (Exception e) {
            log.error("getHasUnitSubTopicList Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }

        return topicList;
    }

    public TopicList getHasUnitSubUnUnitTopicList() {
        TopicList topicList = new TopicList();
        try {
            this.lock.readLock().lockInterruptibly();
            for (Entry<String, Map<String, QueueData>> topicEntry : this.topicQueueTable.entrySet()) {
                String topic = topicEntry.getKey();
                Map<String, QueueData> queueDatas = topicEntry.getValue();
                if (queueDatas != null && queueDatas.size() > 0
                    && !TopicSysFlag.hasUnitFlag(queueDatas.values().iterator().next().getTopicSysFlag())
                    && TopicSysFlag.hasUnitSubFlag(queueDatas.values().iterator().next().getTopicSysFlag())) {
                    topicList.getTopicList().add(topic);
                }
            }
        } catch (Exception e) {
            log.error("getHasUnitSubUnUnitTopicList Exception", e);
        } finally {
            this.lock.readLock().unlock();
        }

        return topicList;
    }
}

class BrokerLiveInfo {
    /**
     * 最近更新时间
     */
    private long lastUpdateTimestamp;
    /**
     * 心跳超时时间
     */
    private long heartbeatTimeoutMillis;
    /**
     * 版本号
     */
    private DataVersion dataVersion;
    private Channel channel;
    private String haServerAddr;

    public BrokerLiveInfo(long lastUpdateTimestamp, long heartbeatTimeoutMillis, DataVersion dataVersion,
        Channel channel,
        String haServerAddr) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
        this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
        this.dataVersion = dataVersion;
        this.channel = channel;
        this.haServerAddr = haServerAddr;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public long getHeartbeatTimeoutMillis() {
        return heartbeatTimeoutMillis;
    }

    public void setHeartbeatTimeoutMillis(long heartbeatTimeoutMillis) {
        this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public String getHaServerAddr() {
        return haServerAddr;
    }

    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }

    @Override
    public String toString() {
        return "BrokerLiveInfo [lastUpdateTimestamp=" + lastUpdateTimestamp + ", dataVersion=" + dataVersion
            + ", channel=" + channel + ", haServerAddr=" + haServerAddr + "]";
    }
}

class BrokerStatusChangeInfo {
    /**
     * 发生变动的 broker key 为 brokerId , value 为 brokerAddress
     */
    Map<Long, String> brokerAddrs;
    /**
     * 取消注册的 brokerAddress
     */
    String offlineBrokerAddr;
    String haBrokerAddr;

    public BrokerStatusChangeInfo(Map<Long, String> brokerAddrs, String offlineBrokerAddr, String haBrokerAddr) {
        this.brokerAddrs = brokerAddrs;
        this.offlineBrokerAddr = offlineBrokerAddr;
        this.haBrokerAddr = haBrokerAddr;
    }

    public Map<Long, String> getBrokerAddrs() {
        return brokerAddrs;
    }

    public void setBrokerAddrs(Map<Long, String> brokerAddrs) {
        this.brokerAddrs = brokerAddrs;
    }

    public String getOfflineBrokerAddr() {
        return offlineBrokerAddr;
    }

    public void setOfflineBrokerAddr(String offlineBrokerAddr) {
        this.offlineBrokerAddr = offlineBrokerAddr;
    }

    public String getHaBrokerAddr() {
        return haBrokerAddr;
    }

    public void setHaBrokerAddr(String haBrokerAddr) {
        this.haBrokerAddr = haBrokerAddr;
    }
}
