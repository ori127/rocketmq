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

import com.alibaba.fastjson.annotation.JSONField;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class ConsumerOrderInfoManager extends ConfigManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final String TOPIC_GROUP_SEPARATOR = "@";
    private static final long CLEAN_SPAN_FROM_LAST = 24 * 3600 * 1000;
    /**
     * key 为 topic@group ,value.key 为 消息队列id value.value 为 OrderInfo 该消费者 在 该消息队列 的 偏移量 , topic 各个消费组 偏移量表
     */
    private ConcurrentHashMap<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, OrderInfo>> table =
        new ConcurrentHashMap<>(128);

    private transient BrokerController brokerController;

    public ConsumerOrderInfoManager() {
    }

    public ConsumerOrderInfoManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, OrderInfo>> getTable() {
        return table;
    }

    public void setTable(ConcurrentHashMap<String, ConcurrentHashMap<Integer, OrderInfo>> table) {
        this.table = table;
    }

    /**
     * not thread safe.
     *
     * @param topic
     * @param group
     * @param queueId
     * @param msgOffsetList
     */
    public int update(String topic, String group, int queueId, List<Long> msgOffsetList) {
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        //根据 topic 和 消费组 获取 该 消费组 该 topic 的 消息队列 偏移量表  不存在 则新建
        ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = table.get(key);
        if (qs == null) {
            qs = new ConcurrentHashMap<>(16);
            ConcurrentHashMap<Integer/*queueId*/, OrderInfo> old = table.putIfAbsent(key, qs);
            if (old != null) {
                qs = old;
            }
        }
        //获取该 消费组 该 topic 该 消息队列 的偏移量信息
        OrderInfo orderInfo = qs.get(queueId);

        // start is same.
        List<Long> simple = OrderInfo.simpleO(msgOffsetList);
        //如果旧的偏移量 和 新的偏移量  第一个 绝对偏移量 是一样的
        //如果新的偏移量 和 就的偏移量 是一样的 FIXME:: 增加消费计数
        if (orderInfo != null && simple.get(0).equals(orderInfo.getOffsetList().get(0))) {
            if (simple.equals(orderInfo.getOffsetList())) {
                orderInfo.setConsumedCount(orderInfo.getConsumedCount() + 1);
            } else {
                // reset, because msgs are changed.
                orderInfo.setConsumedCount(0);
            }
            //更新记录消费时间错 偏移量 集合 提交偏移量的bit
            orderInfo.setLastConsumeTimestamp(System.currentTimeMillis());
            orderInfo.setOffsetList(simple);
            orderInfo.setCommitOffsetBit(0);
        } else {
            //如果旧的偏移量 和 新的偏移量  第一个 绝对偏移量 不是 是一样的 或者 不存在 旧的 偏移量 则设置新 偏移量 更新消息队列的 偏移量 映射
            orderInfo = new OrderInfo();
            orderInfo.setOffsetList(simple);
            orderInfo.setLastConsumeTimestamp(System.currentTimeMillis());
            orderInfo.setConsumedCount(0);
            orderInfo.setCommitOffsetBit(0);

            qs.put(queueId, orderInfo);
        }

        return orderInfo.getConsumedCount();
    }

    public boolean checkBlock(String topic, String group, int queueId, long invisibleTime) {
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        //根据 topic 和 消费组 获取 该 消费组 该 topic 的 消息队列 偏移量表  不存在 则新建
        ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = table.get(key);
        if (qs == null) {
            qs = new ConcurrentHashMap<>(16);
            ConcurrentHashMap<Integer/*queueId*/, OrderInfo> old = table.putIfAbsent(key, qs);
            if (old != null) {
                qs = old;
            }
        }
        //获取该 消费组 该 topic 该 消息队列 的偏移量信息
        OrderInfo orderInfo = qs.get(queueId);

        if (orderInfo == null) {
            return false;
        }
        //顺序消息 最近消费消费 小于 不可见时间 当前顺序消息不可见  block
        boolean isBlock = System.currentTimeMillis() - orderInfo.getLastConsumeTimestamp() < invisibleTime;
        //消息不可见 或者 该顺序消息 还未 完成
        return isBlock && !orderInfo.isDone();
    }

    /**
     * 提交顺序消息的偏移量
     * @param topic
     * @param group
     * @param queueId
     * @param offset
     * @return -1 : illegal, -2 : no need commit, >= 0 : commit
     */
    public long commitAndNext(String topic, String group, int queueId, long offset) {
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        //根据 topic 和 消费组 获取 该 消费组 该 topic 的 消息队列 偏移量表  不存在 则新建
        ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = table.get(key);

        if (qs == null) {
            return offset + 1;
        }
        //获取该 消费组 该 topic 该 消息队列 的偏移量信息
        OrderInfo orderInfo = qs.get(queueId);
        if (orderInfo == null) {
            log.warn("OrderInfo is null, {}, {}, {}", key, offset, orderInfo);
            return offset + 1;
        }
        //获取该顺序消息 消费偏移量
        List<Long> offsetList = orderInfo.getOffsetList();
        if (offsetList == null || offsetList.isEmpty()) {
            log.warn("OrderInfo is empty, {}, {}, {}", key, offset, orderInfo);
            return -1;
        }
        //获取第一个绝对偏移量 计算每个 绝对偏移量 需加上第一个偏移量 找对应 偏移量 的 index
        Long first = offsetList.get(0);
        int i = 0, size = offsetList.size();
        for (; i < size; i++) {
            long temp;
            if (i == 0) {
                temp = first;
            } else {
                temp = first + offsetList.get(i);
            }
            if (offset == temp) {
                break;
            }
        }
        //没有找到
        // not found
        if (i >= size) {
            log.warn("OrderInfo not found commit offset, {}, {}, {}", key, offset, orderInfo);
            return -1;
        }
        //设置对应 偏移量的 bit 进行 提交
        //set bit
        orderInfo.setCommitOffsetBit(orderInfo.getCommitOffsetBit() | (1L << i));
        //若果全部完成 返回 下个偏移量
        if (orderInfo.isDone()) {
            if (size == 1) {
                return offsetList.get(0) + 1;
            } else {
                return offsetList.get(size - 1) + first + 1;
            }
        }
        return -2;
    }

    /**
     * 获取该 消费组 该 topic 该 消息队列 的偏移量信息
     * @param topic
     * @param group
     * @param queueId
     * @return
     */
    public OrderInfo get(String topic, String group, int queueId) {
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = table.get(key);

        if (qs == null) {
            return null;
        }
        //获取该 消费组 该 topic 该 消息队列 的偏移量信息
        return qs.get(queueId);
    }

    public int getConsumeCount(String topic, String group, int queueId) {
        OrderInfo orderInfo = get(topic, group, queueId);
        return orderInfo == null ? 0 : orderInfo.getConsumedCount();
    }

    private void autoClean() {
        if (brokerController == null) {
            return;
        }
        Iterator<Map.Entry<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, OrderInfo>>> iterator =
            this.table.entrySet().iterator();
        //遍历 iterator
        while (iterator.hasNext()) {
            Map.Entry<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, OrderInfo>> entry =
                iterator.next();
            String topicAtGroup = entry.getKey();
            //获取 该 消费组 该 topic 的 消息队列 偏移量表
            ConcurrentHashMap<Integer/*queueId*/, OrderInfo> qs = entry.getValue();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length != 2) {
                continue;
            }
            String topic = arrays[0];
            String group = arrays[1];
            //获取topic 配置信息 如果配置 不存在 移除
            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
            if (topicConfig == null) {
                iterator.remove();
                log.info("Topic not exist, Clean order info, {}:{}", topicAtGroup, qs);
                continue;
            }
            //获取该消费组的订阅配置 信息 不存在 进行 移除
            if (this.brokerController.getSubscriptionGroupManager().getSubscriptionGroupTable().get(group) == null) {
                iterator.remove();
                log.info("Group not exist, Clean order info, {}:{}", topicAtGroup, qs);
                continue;
            }
            //若没有对应消息队列 偏移量进行移除
            if (qs.isEmpty()) {
                iterator.remove();
                log.info("Order table is empty, Clean order info, {}:{}", topicAtGroup, qs);
                continue;
            }

            Iterator<Map.Entry<Integer/*queueId*/, OrderInfo>> qsIterator = qs.entrySet().iterator();
            while (qsIterator.hasNext()) {
                Map.Entry<Integer/*queueId*/, OrderInfo> qsEntry = qsIterator.next();
                //FIXME::?队列 id 超过可读队列数量 移除
                if (qsEntry.getKey() >= topicConfig.getReadQueueNums()) {
                    qsIterator.remove();
                    log.info("Queue not exist, Clean order info, {}:{}, {}", topicAtGroup, entry.getValue(), topicConfig);
                    continue;
                }
                //超过最近消费时间进行移除
                if (System.currentTimeMillis() - qsEntry.getValue().getLastConsumeTimestamp() > CLEAN_SPAN_FROM_LAST) {
                    qsIterator.remove();
                    log.info("Not consume long time, Clean order info, {}:{}, {}", topicAtGroup, entry.getValue(), topicConfig);
                    continue;
                }
            }
        }
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    /**
     * "config/consumerOrderInfo.json";
     * @return
     */
    @Override
    public String configFilePath() {
        if (brokerController != null) {
            return BrokerPathConfigHelper.getConsumerOrderInfoPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        } else {
            return BrokerPathConfigHelper.getConsumerOrderInfoPath("~");
        }
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            ConsumerOrderInfoManager obj = RemotingSerializable.fromJson(jsonString, ConsumerOrderInfoManager.class);
            if (obj != null) {
                this.table = obj.table;
            }
        }
    }

    @Override
    public String encode(boolean prettyFormat) {
        this.autoClean();

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("{\n").append("\t\"table\":{");
        Iterator<Map.Entry<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, OrderInfo>>> iterator =
            this.table.entrySet().iterator();
        int count1 = 0;
        while (iterator.hasNext()) {
            Map.Entry<String/* topic@group*/, ConcurrentHashMap<Integer/*queueId*/, OrderInfo>> entry =
                iterator.next();
            if (count1 > 0) {
                stringBuilder.append(",");
            }
            stringBuilder.append("\n\t\t\"").append(entry.getKey()).append("\":{");
            Iterator<Map.Entry<Integer/*queueId*/, OrderInfo>> qsIterator = entry.getValue().entrySet().iterator();
            int count2 = 0;
            while (qsIterator.hasNext()) {
                Map.Entry<Integer/*queueId*/, OrderInfo> qsEntry = qsIterator.next();
                if (count2 > 0) {
                    stringBuilder.append(",");
                }
                stringBuilder.append("\n\t\t\t").append(qsEntry.getKey()).append(":")
                    .append(qsEntry.getValue().encode());
                count2++;
            }
            stringBuilder.append("\n\t\t}");
            count1++;
        }
        stringBuilder.append("\n\t}").append("\n}");
        return stringBuilder.toString();
    }

    public static class OrderInfo {
        /**
         * offset 偏移量 集合 第一个  offset 为绝对的偏移量 后面的 偏移量为 第一个偏移量的相对偏移量
         */
        private List<Long> offsetList;
        /**
         * consumed count 消费数量
         */
        private int consumedCount;
        /**
         * last consume timestamp 消费的时间戳
         */
        private long lastConsumeTimestamp;
        /**
         * commit offset bit 提交消费的偏移量的 bit 每个 顺序消息 为 1 bit
         */
        private long commitOffsetBit;

        public OrderInfo() {
        }

        public List<Long> getOffsetList() {
            return offsetList;
        }

        public void setOffsetList(List<Long> offsetList) {
            this.offsetList = offsetList;
        }

        public static List<Long> simpleO(List<Long> offsetList) {
            List<Long> simple = new ArrayList<>();
            //如果只有 一个 则 一个为 绝对偏移量
            if (offsetList.size() == 1) {
                simple.addAll(offsetList);
                return simple;
            }
            //超过一个 第一个 为绝对偏移量 后面的 为第一个 偏移量 的 相对偏移量
            Long first = offsetList.get(0);
            simple.add(first);
            for (int i = 1; i < offsetList.size(); i++) {
                simple.add(offsetList.get(i) - first);
            }
            return simple;
        }

        public int getConsumedCount() {
            return consumedCount;
        }

        public void setConsumedCount(int consumedCount) {
            this.consumedCount = consumedCount;
        }

        public long getLastConsumeTimestamp() {
            return lastConsumeTimestamp;
        }

        public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
            this.lastConsumeTimestamp = lastConsumeTimestamp;
        }

        public long getCommitOffsetBit() {
            return commitOffsetBit;
        }

        public void setCommitOffsetBit(long commitOffsetBit) {
            this.commitOffsetBit = commitOffsetBit;
        }

        /**
         * 判断消息是否完成
         * @return
         */
        @JSONField(serialize = false, deserialize = false)
        public boolean isDone() {
            //顺序消费 偏移量为空
            if (offsetList == null || offsetList.isEmpty()) {
                return true;
            }
            int num = offsetList.size();
            //判断是 所有消息 是佛全部 提交
            for (byte i = 0; i < num; i++) {
                if ((commitOffsetBit & (1L << i)) == 0) {
                    return false;
                }
            }
            return true;
        }

        @JSONField(serialize = false, deserialize = false)
        public String encode() {
            StringBuilder sb = new StringBuilder();
            sb.append("{").append("\"c\":").append(getConsumedCount());
            sb.append(",").append("\"cm\":").append(getCommitOffsetBit());
            sb.append(",").append("\"l\":").append(getLastConsumeTimestamp());
            sb.append(",").append("\"o\":[");
            if (getOffsetList() != null) {
                for (int i = 0; i < getOffsetList().size(); i++) {
                    sb.append(getOffsetList().get(i));
                    if (i < getOffsetList().size() - 1) {
                        sb.append(",");
                    }
                }
            }
            sb.append("]").append("}");
            return sb.toString();
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("OrderInfo");
            sb.append("@").append(this.hashCode());
            sb.append("{offsetList=").append(offsetList);
            sb.append(", consumedCount=").append(consumedCount);
            sb.append(", lastConsumeTimestamp=").append(lastConsumeTimestamp);
            sb.append(", commitOffsetBit=").append(commitOffsetBit);
            sb.append(", isDone=").append(isDone());
            sb.append('}');
            return sb.toString();
        }
    }
}
