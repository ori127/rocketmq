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
package org.apache.rocketmq.common.utils;

import org.apache.rocketmq.common.TopicAttributes;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.CQType;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class QueueTypeUtils {
    /**
     * 判读是否是比例消费队列
     * @param topicConfig
     * @return
     */
    public static boolean isBatchCq(Optional<TopicConfig> topicConfig) {
        return Objects.equals(CQType.BatchCQ, getCQType(topicConfig));
    }

    /**
     * 根据 topic 配置 获取 消费队列的类型
     * @param topicConfig
     * @return
     */
    public static CQType getCQType(Optional<TopicConfig> topicConfig) {
        //配置不存在 则是 SimpleCQ
        if (!topicConfig.isPresent()) {
            return CQType.valueOf(TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getDefaultValue());
        }
        //属性名称 queue.type
        String attributeName = TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getName();
        // 获取topic 属性 若果属性为 空则采用 SimpleCQ
        Map<String, String> attributes = topicConfig.get().getAttributes();
        if (attributes == null || attributes.size() == 0) {
            return CQType.valueOf(TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getDefaultValue());
        }
        //根据属性获取 消费 队列 类型
        if (attributes.containsKey(attributeName)) {
            return CQType.valueOf(attributes.get(attributeName));
        } else {
            return CQType.valueOf(TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getDefaultValue());
        }
    }
}