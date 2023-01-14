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
package org.apache.rocketmq.broker.subscription;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class SubscriptionGroupManager extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    /**
     * key 为 组名 , value 为 SubscriptionGroupConfig
     */
    private final ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable =
        new ConcurrentHashMap<String, SubscriptionGroupConfig>(1024);
    /**
     * key 为 组名 ,value.key 为 topic , value.value 为 topicForbidden
     */
    private final ConcurrentMap<String, ConcurrentMap<String, Integer>> forbiddenTable =
        new ConcurrentHashMap<String, ConcurrentMap<String, Integer>>(4);

    private final DataVersion dataVersion = new DataVersion();
    private transient BrokerController brokerController;

    public SubscriptionGroupManager() {
        this.init();
    }

    public SubscriptionGroupManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.init();
    }

    private void init() {
        {
            //TOOLS_CONSUMER 订阅配置
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.TOOLS_CONSUMER_GROUP);
            this.subscriptionGroupTable.put(MixAll.TOOLS_CONSUMER_GROUP, subscriptionGroupConfig);
        }

        {
            //FILTERSRV_CONSUMER 订阅配置
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.FILTERSRV_CONSUMER_GROUP);
            this.subscriptionGroupTable.put(MixAll.FILTERSRV_CONSUMER_GROUP, subscriptionGroupConfig);
        }

        {
            //SELF_TEST_C_GROUP 订阅配置
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.SELF_TEST_CONSUMER_GROUP);
            this.subscriptionGroupTable.put(MixAll.SELF_TEST_CONSUMER_GROUP, subscriptionGroupConfig);
        }

        {
            //CID_ONS-HTTP-PROXY 订阅配置
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.ONS_HTTP_PROXY_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.ONS_HTTP_PROXY_GROUP, subscriptionGroupConfig);
        }

        {
            //CID_ONSAPI_PULL 订阅配置
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_ONSAPI_PULL_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.CID_ONSAPI_PULL_GROUP, subscriptionGroupConfig);
        }

        {
            //CID_ONSAPI_PERMISSION 订阅配置
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_ONSAPI_PERMISSION_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.CID_ONSAPI_PERMISSION_GROUP, subscriptionGroupConfig);
        }

        {
            //CID_ONSAPI_OWNER 订阅配置
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_ONSAPI_OWNER_GROUP);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.CID_ONSAPI_OWNER_GROUP, subscriptionGroupConfig);
        }

        {
            //CID_RMQ_SYS_TRANS 订阅配置
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(MixAll.CID_SYS_RMQ_TRANS);
            subscriptionGroupConfig.setConsumeBroadcastEnable(true);
            this.subscriptionGroupTable.put(MixAll.CID_SYS_RMQ_TRANS, subscriptionGroupConfig);
        }
    }

    /**
     * 更新订阅配置
     * @param config
     */
    public void updateSubscriptionGroupConfig(final SubscriptionGroupConfig config) {
        SubscriptionGroupConfig old = this.subscriptionGroupTable.put(config.getGroupName(), config);
        if (old != null) {
            log.info("update subscription group config, old: {} new: {}", old, config);
        } else {
            log.info("create new subscription group, {}", config);
        }

        long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
        dataVersion.nextVersion(stateMachineVersion);

        this.persist();
    }

    public void updateForbidden(String group, String topic, int forbiddenIndex, boolean setOrClear) {
        if (setOrClear) {
            setForbidden(group, topic, forbiddenIndex);
        } else {
            clearForbidden(group, topic, forbiddenIndex);
        }
    }

    /**
     * 添加 forbiddenIndex 更新 该 组 该 topic 的 forbidden 增加版本进行持久化
     * set the bit value to 1 at the specific index (from 0)
     *
     * @param group
     * @param topic
     * @param forbiddenIndex from 0
     */
    public void setForbidden(String group, String topic, int forbiddenIndex) {
        int topicForbidden = getForbidden(group, topic);
        topicForbidden |= 1 << forbiddenIndex;
        updateForbiddenValue(group, topic, topicForbidden);
    }

    /**
     * 清楚 forbiddenIndex 更新 该 组 该 topic 的 forbidden 增加版本进行持久化
     * clear the bit value to 0 at the specific index (from 0)
     *
     * @param group
     * @param topic
     * @param forbiddenIndex from 0
     */
    public void clearForbidden(String group, String topic, int forbiddenIndex) {
        int topicForbidden = getForbidden(group, topic);
        topicForbidden &= ~(1 << forbiddenIndex);
        updateForbiddenValue(group, topic, topicForbidden);
    }

    public boolean getForbidden(String group, String topic, int forbiddenIndex) {
        int topicForbidden = getForbidden(group, topic);
        int bitForbidden = 1 << forbiddenIndex;
        return (topicForbidden & bitForbidden) == bitForbidden;
    }

    /**
     * 根据该 group 和 topic 获取 topicForbidden
     * @param group
     * @param topic
     * @return
     */
    public int getForbidden(String group, String topic) {
        ConcurrentMap<String, Integer> topicForbiddens = this.forbiddenTable.get(group);
        if (topicForbiddens == null) {
            return 0;
        }
        Integer topicForbidden = topicForbiddens.get(topic);
        if (topicForbidden == null || topicForbidden < 0) {
            topicForbidden = 0;
        }
        return topicForbidden;
    }

    /**
     * 替换 该 组 该 topic 的 forbidden 增加版本进行持久化
     * @param group
     * @param topic
     * @param forbidden
     */
    private void updateForbiddenValue(String group, String topic, Integer forbidden) {
        if (forbidden == null || forbidden <= 0) {
            this.forbiddenTable.remove(group);
            log.info("clear group forbidden, {}@{} ", group, topic);
            return;
        }
        //获取该 组 的 topicsPermMap 如果不存则 创建该  组 的 topicsPermMap
        ConcurrentMap<String, Integer> topicsPermMap = this.forbiddenTable.get(group);
        if (topicsPermMap == null) {
            this.forbiddenTable.putIfAbsent(group, new ConcurrentHashMap<String, Integer>());
            topicsPermMap = this.forbiddenTable.get(group);
        }
        //替换 该 组 该 topic 的 forbidden 增加版本进行持久化
        Integer old = topicsPermMap.put(topic, forbidden);
        if (old != null) {
            log.info("set group forbidden, {}@{} old: {} new: {}", group, topic, old, forbidden);
        } else {
            log.info("set group forbidden, {}@{} old: {} new: {}", group, topic, 0, forbidden);
        }

        this.dataVersion.nextVersion();

        this.persist();
    }

    /**
     * 禁止该组进行消费
     * @param groupName
     */
    public void disableConsume(final String groupName) {
        SubscriptionGroupConfig old = this.subscriptionGroupTable.get(groupName);
        if (old != null) {
            old.setConsumeEnable(false);
            long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
            dataVersion.nextVersion(stateMachineVersion);
        }
    }

    /**
     * 获取该组 的订阅配置 如果是自动创建订阅组 或者 是系统的消费组 则自动创建 订阅组 配置 进行持久化
     * @param group
     * @return
     */
    public SubscriptionGroupConfig findSubscriptionGroupConfig(final String group) {
        //获取该组 的订阅配置
        SubscriptionGroupConfig subscriptionGroupConfig = this.subscriptionGroupTable.get(group);
        if (null == subscriptionGroupConfig) {
            //如果是自动创建订阅组 或者 是系统的消费组 则自动创建 订阅组 配置 进行持久化
            if (brokerController.getBrokerConfig().isAutoCreateSubscriptionGroup() || MixAll.isSysConsumerGroup(group)) {
                subscriptionGroupConfig = new SubscriptionGroupConfig();
                subscriptionGroupConfig.setGroupName(group);
                SubscriptionGroupConfig preConfig = this.subscriptionGroupTable.putIfAbsent(group, subscriptionGroupConfig);
                if (null == preConfig) {
                    log.info("auto create a subscription group, {}", subscriptionGroupConfig.toString());
                }
                long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
                dataVersion.nextVersion(stateMachineVersion);
                this.persist();
            }
        }

        return subscriptionGroupConfig;
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    /**
     *   "/config/subscriptionGroup.json"
     * @return
     */
    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getSubscriptionGroupPath(this.brokerController.getMessageStoreConfig()
            .getStorePathRootDir());
    }

    /**
     * 进行解码 加载配置
     * @param jsonString
     */
    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            SubscriptionGroupManager obj = RemotingSerializable.fromJson(jsonString, SubscriptionGroupManager.class);
            if (obj != null) {
                this.subscriptionGroupTable.putAll(obj.subscriptionGroupTable);
                if (obj.forbiddenTable != null) {
                    this.forbiddenTable.putAll(obj.forbiddenTable);
                }
                this.dataVersion.assignNewOne(obj.dataVersion);
                this.printLoadDataWhenFirstBoot(obj);
            }
        }
    }

    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    /**
     * 日志 记录 subscriptionGroupTable
     * @param sgm
     */
    private void printLoadDataWhenFirstBoot(final SubscriptionGroupManager sgm) {
        Iterator<Entry<String, SubscriptionGroupConfig>> it = sgm.getSubscriptionGroupTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, SubscriptionGroupConfig> next = it.next();
            log.info("load exist subscription group, {}", next.getValue().toString());
        }
    }

    public ConcurrentMap<String, SubscriptionGroupConfig> getSubscriptionGroupTable() {
        return subscriptionGroupTable;
    }

    public ConcurrentMap<String, ConcurrentMap<String, Integer>> getForbiddenTable() {
        return forbiddenTable;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void deleteSubscriptionGroupConfig(final String groupName) {
        SubscriptionGroupConfig old = this.subscriptionGroupTable.remove(groupName);
        this.forbiddenTable.remove(groupName);
        if (old != null) {
            log.info("delete subscription group OK, subscription group:{}", old);
            long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
            dataVersion.nextVersion(stateMachineVersion);
            this.persist();
        } else {
            log.warn("delete subscription group failed, subscription groupName: {} not exist", groupName);
        }
    }

    public void setSubscriptionGroupTable(ConcurrentMap<String, SubscriptionGroupConfig> otherSubscriptionGroupTable) {
        this.subscriptionGroupTable.clear();
        for (String key : otherSubscriptionGroupTable.keySet()) {
            this.subscriptionGroupTable.put(key, otherSubscriptionGroupTable.get(key));
        }
    }
}
