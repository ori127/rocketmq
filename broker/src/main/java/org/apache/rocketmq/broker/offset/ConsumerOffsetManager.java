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
package org.apache.rocketmq.broker.offset;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;


public class ConsumerOffsetManager extends ConfigManager {
    private static final InternalLogger LOG = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    public static final String TOPIC_GROUP_SEPARATOR = "@";
    /**
     * 版本信息
     */
    private DataVersion dataVersion = new DataVersion();
    /**
     * key 为 topic@group ,value.key 为 消息队列id value.value 为 topic 该消费者 在 该消息队列 的 偏移量 , topic 各个消费组 偏移量表
     */
    protected ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer, Long>> offsetTable =
        new ConcurrentHashMap<String, ConcurrentMap<Integer, Long>>(512);

    protected transient BrokerController brokerController;
    /**
     * 版本改变计数
     */
    private transient AtomicLong versionChangeCounter = new AtomicLong(0);

    public ConsumerOffsetManager() {
    }

    public ConsumerOffsetManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }
    /**
     * 据遍历偏移量表表 找对应 group 删除
     * @param group
     */
    public void cleanOffset(String group) {
        //根据遍历偏移量表表 找对应 group 删除
        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            if (topicAtGroup.contains(group)) {
                String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
                if (arrays.length == 2 && group.equals(arrays[1])) {
                    it.remove();
                    LOG.warn("Clean group's offset, {}, {}", topicAtGroup, next.getValue());
                }
            }
        }
    }

    /**
     * 根据遍历偏移量表表 找对应 topic 删除
     * @param topic
     */
    public void cleanOffsetByTopic(String topic) {
        //根据遍历偏移量表表 找对应 topic 删除
        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            if (topicAtGroup.contains(topic)) {
                String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
                if (arrays.length == 2 && topic.equals(arrays[0])) {
                    it.remove();
                    LOG.warn("Clean topic's offset, {}, {}", topicAtGroup, next.getValue());
                }
            }
        }
    }

    public void scanUnsubscribedTopic() {
        //遍历偏移量表 如果 消费组 和 该 topic 不存在订阅关系 则进行移除
        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length == 2) {
                String topic = arrays[0];
                String group = arrays[1];
                //消费组 和 该 topic 不存在订阅关系
                // 遍历该 topic 该消费组 各个消息队列的 的 存储最小偏移量 比 table (内存) 中的 小  进行移除
                if (null == brokerController.getConsumerManager().findSubscriptionData(group, topic)
                    && this.offsetBehindMuchThanData(topic, next.getValue())) {
                    it.remove();
                    LOG.warn("remove topic offset, {}", topicAtGroup);
                }
            }
        }
    }

    /**
     * 遍历该 topic 该消费组 各个消息队列的 的 存储最小偏移量 是否 比 table (内存) 中的 小
     * @param topic
     * @param table
     * @return
     */
    private boolean offsetBehindMuchThanData(final String topic, ConcurrentMap<Integer, Long> table) {
        //it 为 该 topic 该消费组的偏移量
        Iterator<Entry<Integer, Long>> it = table.entrySet().iterator();
        boolean result = !table.isEmpty();
        //遍历该 topic 该消费组 各个消息队列的 的 存储偏最小偏移量 比 table (内存) 中的 小
        while (it.hasNext() && result) {
            Entry<Integer, Long> next = it.next();
            long minOffsetInStore = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, next.getKey());
            long offsetInPersist = next.getValue();
            result = offsetInPersist <= minOffsetInStore;
        }

        return result;
    }

    /**
     * 获取该消费组 消费的 topic
     * @param group
     * @return
     */
    public Set<String> whichTopicByConsumer(final String group) {
        Set<String> topics = new HashSet<String>();
        //遍历offsetTable 获取该 消费组 的 topic
        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length == 2) {
                if (group.equals(arrays[1])) {
                    topics.add(arrays[0]);
                }
            }
        }

        return topics;
    }

    /**
     * 遍历 offsetTable 获取该 topic 的 消费组
     * @param topic
     * @return
     */
    public Set<String> whichGroupByTopic(final String topic) {
        Set<String> groups = new HashSet<String>();
        //遍历offsetTable 获取该 topic 的 消费组
        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length == 2) {
                if (topic.equals(arrays[0])) {
                    groups.add(arrays[1]);
                }
            }
        }

        return groups;
    }

    /**
     * 遍历offsetTable 获取每个消费 topic 集合
     * @return
     */
    public Map<String, Set<String>> getGroupTopicMap() {
        Map<String, Set<String>> retMap = new HashMap<String, Set<String>>(128);
        //遍历offsetTable 获取每个消费 topic 集合
        for (String key : this.offsetTable.keySet()) {
            String[] arr = key.split(TOPIC_GROUP_SEPARATOR);
            if (arr.length == 2) {
                String topic = arr[0];
                String group = arr[1];

                Set<String> topics = retMap.get(group);
                if (topics == null) {
                    topics = new HashSet<String>(8);
                    retMap.put(group, topics);
                }

                topics.add(topic);
            }
        }

        return retMap;
    }

    public void commitOffset(final String clientHost, final String group, final String topic, final int queueId,
        final long offset) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        this.commitOffset(clientHost, key, queueId, offset);
    }

    private void commitOffset(final String clientHost, final String key, final int queueId, final long offset) {
        //获取 该topic 消费组的 偏移量 不存在 则 添加 该消息 队列的偏移量
        ConcurrentMap<Integer, Long> map = this.offsetTable.get(key);
        if (null == map) {
            map = new ConcurrentHashMap<Integer, Long>(32);
            map.put(queueId, offset);
            this.offsetTable.put(key, map);
        } else {
            //覆盖该消息队列的偏移量
            Long storeOffset = map.put(queueId, offset);
            if (storeOffset != null && offset < storeOffset) {
                LOG.warn("[NOTIFYME]update consumer offset less than store. clientHost={}, key={}, queueId={}, requestOffset={}, storeOffset={}", clientHost, key, queueId, offset, storeOffset);
            }
        }
        //版本计数为消费偏移量更新步调的倍数 则更新 版本
        if (versionChangeCounter.incrementAndGet() % brokerController.getBrokerConfig().getConsumerOffsetUpdateVersionStep() == 0) {
            long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
            dataVersion.nextVersion(stateMachineVersion);
        }
    }

    /**
     * 获取 该topic 消费组的 消息队列 获取 偏移量
     * @param group
     * @param topic
     * @param queueId
     * @return
     */
    public long queryOffset(final String group, final String topic, final int queueId) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        //获取 该topic 消费组的 偏移量 然后再根据 消息队列 获取 偏移量
        ConcurrentMap<Integer, Long> map = this.offsetTable.get(key);
        if (null != map) {
            Long offset = map.get(queueId);
            if (offset != null) {
                return offset;
            }
        }

        return -1;
    }

    public String encode() {
        return this.encode(false);
    }

    /**
     *  "/config/consumerOffset.json"
     * @return
     */
    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getConsumerOffsetPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            ConsumerOffsetManager obj = RemotingSerializable.fromJson(jsonString, ConsumerOffsetManager.class);
            if (obj != null) {
                this.offsetTable = obj.offsetTable;
                this.dataVersion = obj.dataVersion;
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, Long>> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> offsetTable) {
        this.offsetTable = offsetTable;
    }

    /**
     * 获取该 topic 消费者中 最小的 偏移量
     * @param topic
     * @param filterGroups
     * @return
     */
    public Map<Integer, Long> queryMinOffsetInAllGroup(final String topic, final String filterGroups) {

        Map<Integer, Long> queueMinOffset = new HashMap<Integer, Long>();
        Set<String> topicGroups = this.offsetTable.keySet();
        //获取 offsetTable key 去掉 filterGroups 的 消费组 的 偏移量信息
        if (!UtilAll.isBlank(filterGroups)) {
            for (String group : filterGroups.split(",")) {
                Iterator<String> it = topicGroups.iterator();
                while (it.hasNext()) {
                    if (group.equals(it.next().split(TOPIC_GROUP_SEPARATOR)[1])) {
                        it.remove();
                    }
                }
            }
        }
        //遍历 offsetTable 该 topic 消费 中最小 的消费 偏移量
        for (Map.Entry<String, ConcurrentMap<Integer, Long>> offSetEntry : this.offsetTable.entrySet()) {
            String topicGroup = offSetEntry.getKey();
            String[] topicGroupArr = topicGroup.split(TOPIC_GROUP_SEPARATOR);
            //找到对应 topic 的 偏移量信息
            if (topic.equals(topicGroupArr[0])) {
                for (Entry<Integer, Long> entry : offSetEntry.getValue().entrySet()) {
                    //获取该 topic 该消息 队列 最小的 偏移量
                    long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, entry.getKey());
                    if (entry.getValue() >= minOffset) {
                        //获取该 消息队列 消费者 的最小 偏移量 如果不存在 则 该消费者的 偏移量最小 如果存在 则判断 其他的消费者 的偏移量 和 该消费者 偏移量 最小 的偏移量
                        Long offset = queueMinOffset.get(entry.getKey());
                        if (offset == null) {
                            queueMinOffset.put(entry.getKey(), Math.min(Long.MAX_VALUE, entry.getValue()));
                        } else {
                            queueMinOffset.put(entry.getKey(), Math.min(entry.getValue(), offset));
                        }
                    }
                }
            }

        }
        return queueMinOffset;
    }

    /**
     * 获取 topic 消费组 的队列偏移量
     * @param group
     * @param topic
     * @return
     */
    public Map<Integer, Long> queryOffset(final String group, final String topic) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        return this.offsetTable.get(key);
    }

    /**
     * 克隆 消费组偏移量
     * @param srcGroup
     * @param destGroup
     * @param topic
     */
    public void cloneOffset(final String srcGroup, final String destGroup, final String topic) {
        ConcurrentMap<Integer, Long> offsets = this.offsetTable.get(topic + TOPIC_GROUP_SEPARATOR + srcGroup);
        if (offsets != null) {
            this.offsetTable.put(topic + TOPIC_GROUP_SEPARATOR + destGroup, new ConcurrentHashMap<Integer, Long>(offsets));
        }
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }

    /**
     * 从 offsetTable 移除 该消费组 的偏移量
     * @param group
     */
    public void removeOffset(final String group) {
        //从 offsetTable 移除 该消费组 的偏移量
        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            if (topicAtGroup.contains(group)) {
                String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
                if (arrays.length == 2 && group.equals(arrays[1])) {
                    it.remove();
                    LOG.warn("clean group offset {}", topicAtGroup);
                }
            }
        }

    }

}
