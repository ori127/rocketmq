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

package org.apache.rocketmq.common.queue;

import java.util.LinkedList;
import java.util.Queue;

/**
 * 由不LinkedList实现 非线程安全
 * not thread safe
 */
public class RoundQueue<E> {
    /**
     * 队列
     */
    private Queue<E> queue;
    /**
     * 容量大小
     */
    private int capacity;

    public RoundQueue(int capacity) {
        this.capacity = capacity;
        queue = new LinkedList<E>();
    }

    /**
     * 添加元素到 队列中 返回是否添加 成功
     * @param e
     * @return
     */
    public boolean put(E e) {
        boolean ok = false;
        //判读是否已经在队列中,如果不存在 poll 一个 ,将新的元素添加到队列中 返回添加成功
        if (!queue.contains(e)) {
            if (queue.size() >= capacity) {
                queue.poll();
            }
            queue.add(e);
            ok = true;
        }

        return ok;
    }
}
