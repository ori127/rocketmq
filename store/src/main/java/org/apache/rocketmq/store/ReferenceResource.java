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

import java.util.concurrent.atomic.AtomicLong;

/**
 * 资源引用
 */
public abstract class ReferenceResource {
    /**
     * 引用计数
     */
    protected final AtomicLong refCount = new AtomicLong(1);
    /**
     * 是否可用
     */
    protected volatile boolean available = true;
    /**
     * 是否清理完毕
     */
    protected volatile boolean cleanupOver = false;
    /**
     * 记录关闭的时间戳
     */
    private volatile long firstShutdownTimestamp = 0;
    /**
     * 检查是否可用 增加引用计数
     * @return
     */
    public synchronized boolean hold() {
        //是否可用 可用增加计数
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    /**
     * 关闭 将设置成 不可用 记录关闭时间 进行释放
     * @param intervalForcibly
     */
    public void shutdown(final long intervalForcibly) {
        //如果可用 将设置成 不可用 记录关闭时间 进行释放
        if (this.available) {
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            this.release();
        } else if (this.getRefCount() > 0) {
            //如果不可用 并且有 引用计数 如果第一次 关闭 超过 intervalForcibly 则将引用计数  设置为 负数 进行释放
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    /**
     * 释放
     */
    public void release() {
        //减少应引用计数
        long value = this.refCount.decrementAndGet();
        //如有还有引用 则 返回 没有引用 调用清理方法
        if (value > 0)
            return;

        synchronized (this) {
            //调用清理方法 由子类实现
            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    /**
     * 是否清理结束 引用小于 等于 0 ,并且清理结束
     * @return
     */
    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
