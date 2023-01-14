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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.store.queue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.ConcurrentHashMapUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * QueueOffsetAssigner is a component for assigning offsets for queues.
 */
public class QueueOffsetAssigner {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * key 为 topic-queueId , value 为偏移量 简单队列的偏移量
     */
    private ConcurrentMap<String, Long> topicQueueTable = new ConcurrentHashMap<>(1024);
    /**
     * key 为 topic-queueId , value 为偏移量 批量队列的偏移量
     */
    private ConcurrentMap<String, Long> batchTopicQueueTable = new ConcurrentHashMap<>(1024);
    /**
     * key 为 topic-queueId , value 为偏移量 轻量级队列的偏移量
     */
    private ConcurrentMap<String/* topic-queueid */, Long/* offset */> lmqTopicQueueTable = new ConcurrentHashMap<>(1024);

    /**
     * 初始化 简单消费队列  topic-queueId 偏移量 加上 消息偏移量
     * @param topicQueueKey
     * @param messageNum
     * @return
     */
    public long assignQueueOffset(String topicQueueKey, short messageNum) {
        Long queueOffset = ConcurrentHashMapUtils.computeIfAbsent(this.topicQueueTable, topicQueueKey, k -> 0L);
        this.topicQueueTable.put(topicQueueKey, queueOffset + messageNum);
        return queueOffset;
    }

    /**
     * 更新  简单消费队列 topic-queueId 偏移量
     * @param topicQueueKey
     * @param offset
     */
    public void updateQueueOffset(String topicQueueKey, long offset) {
        this.topicQueueTable.put(topicQueueKey, offset);
    }
    /**
     * 初始化 批量消费队列  topic-queueId 偏移量 加上消息偏移量
     * @param topicQueueKey
     * @param messageNum
     * @return
     */
    public long assignBatchQueueOffset(String topicQueueKey, short messageNum) {
        Long topicOffset = ConcurrentHashMapUtils.computeIfAbsent(this.batchTopicQueueTable, topicQueueKey, k -> 0L);
        this.batchTopicQueueTable.put(topicQueueKey, topicOffset + messageNum);
        return topicOffset;
    }
    /**
     * 初始化 轻量级  topic-queueId 偏移量 加上消息偏移量
     * @param topicQueueKey
     * @param messageNum
     * @return
     */
    public long assignLmqOffset(String topicQueueKey, short messageNum) {
        Long topicOffset = ConcurrentHashMapUtils.computeIfAbsent(this.lmqTopicQueueTable, topicQueueKey, k -> 0L);
        this.lmqTopicQueueTable.put(topicQueueKey, topicOffset + messageNum);
        return topicOffset;
    }

    /**
     * 当前简单消费队列的偏移量
     * @param topicQueueKey
     * @return
     */
    public long currentQueueOffset(String topicQueueKey) {
        return this.topicQueueTable.get(topicQueueKey);
    }
    /**
     * 当前批量消费队列的偏移量
     * @param topicQueueKey
     * @return
     */
    public long currentBatchQueueOffset(String topicQueueKey) {
        return this.batchTopicQueueTable.get(topicQueueKey);
    }
    /**
     * 当前轻量级消费队列的偏移量
     * @param topicQueueKey
     * @return
     */
    public long currentLmqOffset(String topicQueueKey) {
        return this.lmqTopicQueueTable.get(topicQueueKey);
    }

    /**
     * 移除  topic-queueId 对应的偏移量
     * @param topic
     * @param queueId
     */
    public synchronized void remove(String topic, Integer queueId) {
        String topicQueueKey = topic + "-" + queueId;
        // Beware of thread-safety
        this.topicQueueTable.remove(topicQueueKey);
        this.batchTopicQueueTable.remove(topicQueueKey);
        this.lmqTopicQueueTable.remove(topicQueueKey);

        log.info("removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}", topic, queueId);
    }

    /**
     * 设置 简单消费队列 topic-queueId 偏移量
     * @param topicQueueTable
     */
    public void setTopicQueueTable(ConcurrentMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }

    /**
     * 获取 简单消费队列的 偏移量
     * @return
     */
    public ConcurrentMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }
    /**
     * 设置 简单消费队列 topic-queueId 偏移量
     * @param batchTopicQueueTable
     */
    public void setBatchTopicQueueTable(ConcurrentMap<String, Long> batchTopicQueueTable) {
        this.batchTopicQueueTable = batchTopicQueueTable;
    }
}