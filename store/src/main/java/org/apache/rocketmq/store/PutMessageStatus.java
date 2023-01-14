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
package org.apache.rocketmq.store;

public enum PutMessageStatus {
    /**
     * 成功
     */
    PUT_OK,
    /**
     * 刷新磁盘超时
     */
    FLUSH_DISK_TIMEOUT,
    /**
     * 刷新 备 超时
     */
    FLUSH_SLAVE_TIMEOUT,
    /**
     * 备不可用
     */
    SLAVE_NOT_AVAILABLE,
    /**
     * 服务不可用
     */
    SERVICE_NOT_AVAILABLE,
    /**
     * 创建文件映射失败
     */
    CREATE_MAPPED_FILE_FAILED,
    /**
     * 消息非法
     */
    MESSAGE_ILLEGAL,
    /**
     * 属性超过大小
     */
    PROPERTIES_SIZE_EXCEEDED,
    /**
     * 操作系统缓存页面繁忙
     */
    OS_PAGE_CACHE_BUSY,
    /**
     * 位置
     */
    UNKNOWN_ERROR,
    IN_SYNC_REPLICAS_NOT_ENOUGH,
    /**
     * 投放远程 broker 失败
     */
    PUT_TO_REMOTE_BROKER_FAIL,
    LMQ_CONSUME_QUEUE_NUM_EXCEEDED,
    WHEEL_TIMER_FLOW_CONTROL,
    WHEEL_TIMER_MSG_ILLEGAL,
    WHEEL_TIMER_NOT_ENABLE
}
