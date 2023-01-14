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
package org.apache.rocketmq.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public abstract class ServiceThread implements Runnable {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private static final long JOIN_TIME = 90 * 1000;
    /**
     * 线程对象
     */
    protected Thread thread;
    /**
     * 等待点
     */
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);
    /**
     * 是否已经通知 一开始为 false
     */
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);
    /**
     * 是否已经停止
     */
    protected volatile boolean stopped = false;
    /**
     * 是否是后台线程
     */
    protected boolean isDaemon = false;

    //Make it able to restart the thread
    /**
     * 是否已经启动
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

    public ServiceThread() {

    }

    public abstract String getServiceName();

    /**
     * 创建线程进行启动
     */
    public void start() {
        log.info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        //将启动状态设置 true
        if (!started.compareAndSet(false, true)) {
            return;
        }
        stopped = false;
        this.thread = new Thread(this, getServiceName());
        this.thread.setDaemon(isDaemon);
        this.thread.start();
    }

    /**
     * 进行关闭
     */
    public void shutdown() {
        this.shutdown(false);
    }

    /**
     * 进行关闭
     * @param interrupt
     */
    public void shutdown(final boolean interrupt) {
        log.info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        //将启动状态 修改 成 false 关闭状态 修改成 true
        if (!started.compareAndSet(true, false)) {
            return;
        }
        this.stopped = true;
        log.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);
        //将线程通知标志 修改成 ture 进行 notify
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        try {
            //需要中断 则对线程进行中断
            if (interrupt) {
                this.thread.interrupt();
            }

            long beginTime = System.currentTimeMillis();
            //如果线程不是后台现在则 等待线程结束 JOIN_TIME
            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJoinTime());
            }
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread " + this.getServiceName() + " elapsed time(ms) " + elapsedTime + " "
                + this.getJoinTime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    public long getJoinTime() {
        return JOIN_TIME;
    }

    @Deprecated
    public void stop() {
        this.stop(false);
    }

    /**
     * 进行停止
     * @param interrupt
     */
    @Deprecated
    public void stop(final boolean interrupt) {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("stop thread " + this.getServiceName() + " interrupt " + interrupt);
        //将线程通知标志 修改成 ture 进行 notify
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
        //需要中断则进行中断 中断线程
        if (interrupt) {
            this.thread.interrupt();
        }
    }

    /**
     * 标记线程停止
     */
    public void makeStop() {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("makestop thread " + this.getServiceName());
    }

    /**
     * 进行唤醒 修改通知标志 进行 notify
     */
    public void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
    }

    /**
     * 等待运行 如果已经通知 修改通知标志 进行等待结束回调
     * @param interval
     */
    protected void waitForRunning(long interval) {
        //如果已经通知 修改通知标志 进行等待结束回调
        if (hasNotified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }
        //还未通知 将waitPoint重置
        //entry to wait
        waitPoint.reset();
        //进行等待
        try {
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } finally {
            //将通知标志修改成 false 进行等待结束回调
            hasNotified.set(false);
            this.onWaitEnd();
        }
    }

    /**
     * 等待结束回调
     */
    protected void onWaitEnd() {
    }

    public boolean isStopped() {
        return stopped;
    }

    public boolean isDaemon() {
        return isDaemon;
    }

    public void setDaemon(boolean daemon) {
        isDaemon = daemon;
    }
}
