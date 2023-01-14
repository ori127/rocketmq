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
package org.apache.rocketmq.broker.topic;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;

/**
 * 轻量消息队列 topic 配置管理
 */
public class LmqTopicConfigManager extends TopicConfigManager {
    public LmqTopicConfigManager(BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public TopicConfig selectTopicConfig(final String topic) {
        //是否是轻量消息队列 如果是 则直接返回 simpleLmqTopicConfig 否者从 配置管理中获取 该topic 配置
        if (MixAll.isLmq(topic)) {
            return simpleLmqTopicConfig(topic);
        }
        return super.selectTopicConfig(topic);
    }

    @Override
    public void updateTopicConfig(final TopicConfig topicConfig) {
        //轻量消息队列 无法进行更新
        if (topicConfig == null || MixAll.isLmq(topicConfig.getTopicName())) {
            return;
        }
        super.updateTopicConfig(topicConfig);
    }

    private TopicConfig simpleLmqTopicConfig(String topic) {
        return new TopicConfig(topic, 1, 1, PermName.PERM_READ | PermName.PERM_WRITE);
    }

}
