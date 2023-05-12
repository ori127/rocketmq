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
package org.apache.rocketmq.store.timer;

public class Slot {
    /**
     * slot 大小
     */
    public static final short SIZE = 32;
    /**
     * 8个字节 该slot 对应的开始时间
     */
    public final long timeMs;
    /**
     * 8个字节 16个字节
     */
    public final long firstPos;
    /**
     * 8个字节 24个字节
     */
    public final long lastPos;
    /**
     * 4个字节 28个字节
     */
    public final int num;
    /**
     * 4个 字节 还未使用
     */
    public final int magic; //no use now, just keep it

    public Slot(long timeMs, long firstPos, long lastPos) {
        this.timeMs = timeMs;
        this.firstPos = firstPos;
        this.lastPos = lastPos;
        this.num = 0;
        this.magic = 0;
    }

    public Slot(long timeMs, long firstPos, long lastPos, int num, int magic) {
        this.timeMs = timeMs;
        this.firstPos = firstPos;
        this.lastPos = lastPos;
        this.num = num;
        this.magic = magic;
    }
}
