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
package org.apache.rocketmq.common.topic;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.HashSet;
import java.util.Set;

public class TopicValidator {

    public static final String AUTO_CREATE_TOPIC_KEY_TOPIC = "TBW102"; // Will be created at broker when isAutoCreateTopicEnable
    /**
     * 延迟消息 topic
     */
    public static final String RMQ_SYS_SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";
    public static final String RMQ_SYS_BENCHMARK_TOPIC = "BenchmarkTest";


    /**
     * 半消息事务消息的 tpoic
     */
    public static final String RMQ_SYS_TRANS_HALF_TOPIC = "RMQ_SYS_TRANS_HALF_TOPIC";
    /**
     * 用于 消息 跟踪 的 topic
     */
    public static final String RMQ_SYS_TRACE_TOPIC = "RMQ_SYS_TRACE_TOPIC";
    /**

     */
    public static final String RMQ_SYS_TRANS_OP_HALF_TOPIC = "RMQ_SYS_TRANS_OP_HALF_TOPIC";
    /**
     * 事务检查最大的次数 的 topic
     */
    public static final String RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC = "TRANS_CHECK_MAX_TIME_TOPIC";
    /**
     * 自测 topic
     */
    public static final String RMQ_SYS_SELF_TEST_TOPIC = "SELF_TEST_TOPIC";
    /**
     * OFFSET_MOVED_EVENT topic
     */
    public static final String RMQ_SYS_OFFSET_MOVED_EVENT = "OFFSET_MOVED_EVENT";
    /**
     * 系统消费组前缀
     */
    public static final String SYSTEM_TOPIC_PREFIX = "rmq_sys_";
    public static final String SYNC_BROKER_MEMBER_GROUP_PREFIX = SYSTEM_TOPIC_PREFIX + "SYNC_BROKER_MEMBER_";
    /**
     * 验证字符的 bit Map
     */
    public static final boolean[] VALID_CHAR_BIT_MAP = new boolean[128];
    /**
     * topic 最大长度
     */
    private static final int TOPIC_MAX_LENGTH = 127;
    /**
     * 系统 topic 集合
     */
    private static final Set<String> SYSTEM_TOPIC_SET = new HashSet<String>();

    /**
     * 不允许发送消息的 topic 集合
     * Topics'set which client can not send msg!
     */
    private static final Set<String> NOT_ALLOWED_SEND_TOPIC_SET = new HashSet<String>();

    static {
        SYSTEM_TOPIC_SET.add(AUTO_CREATE_TOPIC_KEY_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_SCHEDULE_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_BENCHMARK_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_TRANS_HALF_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_TRACE_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_TRANS_OP_HALF_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_SELF_TEST_TOPIC);
        SYSTEM_TOPIC_SET.add(RMQ_SYS_OFFSET_MOVED_EVENT);

        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_SCHEDULE_TOPIC);
        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_TRANS_HALF_TOPIC);
        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_TRANS_OP_HALF_TOPIC);
        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC);
        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_SELF_TEST_TOPIC);
        NOT_ALLOWED_SEND_TOPIC_SET.add(RMQ_SYS_OFFSET_MOVED_EVENT);

        // regex: ^[%|a-zA-Z0-9_-]+$
        // %
        VALID_CHAR_BIT_MAP['%'] = true;
        // -
        VALID_CHAR_BIT_MAP['-'] = true;
        // _
        VALID_CHAR_BIT_MAP['_'] = true;
        // |
        VALID_CHAR_BIT_MAP['|'] = true;
        for (int i = 0; i < VALID_CHAR_BIT_MAP.length; i++) {
            if (i >= '0' && i <= '9') {
                // 0-9
                VALID_CHAR_BIT_MAP[i] = true;
            } else if (i >= 'A' && i <= 'Z') {
                // A-Z
                VALID_CHAR_BIT_MAP[i] = true;
            } else if (i >= 'a' && i <= 'z') {
                // a-z
                VALID_CHAR_BIT_MAP[i] = true;
            }
        }
    }

    /**
     * 判断 topic 或 group 是否非法
     * @param str
     * @return
     */
    public static boolean isTopicOrGroupIllegal(String str) {
        int strLen = str.length();
        int len = VALID_CHAR_BIT_MAP.length;
        boolean[] bitMap = VALID_CHAR_BIT_MAP;
        for (int i = 0; i < strLen; i++) {
            char ch = str.charAt(i);
            if (ch >= len || !bitMap[ch]) {
                return true;
            }
        }
        return false;
    }

    /**
     * 验证对应的topic 并且 设置 对应的服务状态
     * @param topic
     * @param response
     * @return
     */
    public static boolean validateTopic(String topic, RemotingCommand response) {

        if (UtilAll.isBlank(topic)) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The specified topic is blank.");
            return false;
        }

        if (isTopicOrGroupIllegal(topic)) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The specified topic contains illegal characters, allowing only ^[%|a-zA-Z0-9_-]+$");
            return false;
        }

        if (topic.length() > TOPIC_MAX_LENGTH) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The specified topic is longer than topic max length.");
            return false;
        }

        return true;
    }

    /**
     * 判断是否是系统topic
     * @param topic
     * @param response
     * @return
     */
    public static boolean isSystemTopic(String topic, RemotingCommand response) {
        if (isSystemTopic(topic)) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("The topic[" + topic + "] is conflict with system topic.");
            return true;
        }
        return false;
    }

    /**
     * 判断该topic 是否包含在 系统topic
     * @param topic
     * @return
     */
    public static boolean isSystemTopic(String topic) {
        return SYSTEM_TOPIC_SET.contains(topic) || topic.startsWith(SYSTEM_TOPIC_PREFIX);
    }

    /**
     * 判断该topic 是否允许发送
     * @param topic
     * @return
     */
    public static boolean isNotAllowedSendTopic(String topic) {
        return NOT_ALLOWED_SEND_TOPIC_SET.contains(topic);
    }
    /**
     * 判断该topic 是否允许发送 并且 设置 对应的服务状态
     * @param topic
     * @return
     */
    public static boolean isNotAllowedSendTopic(String topic, RemotingCommand response) {
        if (isNotAllowedSendTopic(topic)) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("Sending message to topic[" + topic + "] is forbidden.");
            return true;
        }
        return false;
    }

    /**
     * 添加到系统topic集合
     * @param systemTopic
     */
    public static void addSystemTopic(String systemTopic) {
        SYSTEM_TOPIC_SET.add(systemTopic);
    }

    public static Set<String> getSystemTopicSet() {
        return SYSTEM_TOPIC_SET;
    }

    public static Set<String> getNotAllowedSendTopicSet() {
        return NOT_ALLOWED_SEND_TOPIC_SET;
    }
}
