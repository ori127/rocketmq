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
package org.apache.rocketmq.client.consumer;

public enum PopStatus {
    /**
     * 找到消息
     * Founded
     */
    FOUND,
    /**
     * 轮训时间结束没有获取新消息
     * No new message can be pull after polling time out
     * delete after next realease
     */
    NO_NEW_MSG,
    /**
     * 轮训池已经满了
     * polling pool is full, do not try again immediately.
     */
    POLLING_FULL,
    /**
     * 轮训时间到了 但是没有消息
     * polling time out but no message find
     */
    POLLING_NOT_FOUND
}
