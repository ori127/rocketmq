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
package org.apache.rocketmq.store.ha;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.HashMap;

public class WaitNotifyObject {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * key 为线程id ,value 为 通知标记
     */
    protected final HashMap<Long/* thread id */, Boolean/* notified */> waitingThreadTable =
        new HashMap<Long, Boolean>(16);
    /**
     * 是否已经通知
     */
    protected volatile boolean hasNotified = false;

    public void wakeup() {
        synchronized (this) {
            //还没有通知,标记已经通知, 然后进行唤醒
            if (!this.hasNotified) {
                this.hasNotified = true;
                this.notify();
            }
        }
    }

    public void waitForRunning(long interval) {
        synchronized (this) {
            //如果已经通知,将标记改成还没有通知,调用waitEnd
            if (this.hasNotified) {
                this.hasNotified = false;
                this.onWaitEnd();
                return;
            }
            //未被通知 则进行 等待 interval 毫秒 然后 将标记改成还没有通知 ,调用waitEnd
            try {
                this.wait(interval);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            } finally {
                this.hasNotified = false;
                this.onWaitEnd();
            }
        }
    }

    protected void onWaitEnd() {
    }

    public void wakeupAll() {
        synchronized (this) {
            boolean needNotify = false;
            //遍历线程表,如果有需要唤醒的 则进唤醒
            for (Boolean value : this.waitingThreadTable.values()) {
                needNotify = needNotify || !value;
                value = true;
            }

            if (needNotify) {
                this.notifyAll();
            }
        }
    }

    public void allWaitForRunning(long interval) {
        long currentThreadId = Thread.currentThread().getId();
        synchronized (this) {
            //获取取当前线程通知标记 ,如果已经通知,将标记改成还没有通知,调用waitEnd
            Boolean notified = this.waitingThreadTable.get(currentThreadId);
            if (notified != null && notified) {
                this.waitingThreadTable.put(currentThreadId, false);
                this.onWaitEnd();
                return;
            }
            //当前线程通知标记,还未通知则进行等待,将标记改成还没有通知,调用waitEnd
            try {
                this.wait(interval);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            } finally {
                this.waitingThreadTable.put(currentThreadId, false);
                this.onWaitEnd();
            }
        }
    }

    /**
     * 将线程从等待表中进行移除
     */
    public void removeFromWaitingThreadTable() {
        //获取线程id 将该线程的 标记进行移除
        long currentThreadId = Thread.currentThread().getId();
        synchronized (this) {
            this.waitingThreadTable.remove(currentThreadId);
        }
    }
}
