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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely extends AbstractAllocateMessageQueueStrategy {

    public AllocateMessageQueueAveragely() {
        log = ClientLogger.getLog();
    }

    public AllocateMessageQueueAveragely(InternalLogger log) {
        super(log);
    }

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!check(consumerGroup, currentCID, mqAll, cidAll)) {
            return result;
        }
        /*
        比如总共 96 队列   9 客户端
        index 0 [0-11)
        index 1 [11-22)
        index 2 [22-33)
        index 3 [33-44)
        index 4 [44-55)
        index 5 [55-66)
        index 6 [66-76)
        index 7 [76-86)
        index 8 [86-96)
         */
        int index = cidAll.indexOf(currentCID);
        //余
        int mod = mqAll.size() % cidAll.size();
        //队列数量比 客户端少那就 每个 客户端一个
        //如果有余数 且 余数 比客户端数量 多 那就 先每个可端 平均分 然后 多余的 按顺序分配
        //如果没有余数 或者余数 比当前客户端 index 高 那表示剩余的 队列已经分配完 那就客户端平均分配
        int averageSize =
            mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                + 1 : mqAll.size() / cidAll.size());
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        //计算该 index 范围防止溢出
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
