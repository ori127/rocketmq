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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.store.config.MessageStoreConfig;
/**
 * 流量监控
 */
public class FlowMonitor extends ServiceThread {
    /**
     * 传输字节
     */
    private final AtomicLong transferredByte = new AtomicLong(0L);
    /**
     * 一秒内的传输字节
     */
    private volatile long transferredByteInSecond;
    protected MessageStoreConfig messageStoreConfig;

    public FlowMonitor(MessageStoreConfig messageStoreConfig) {
        this.messageStoreConfig = messageStoreConfig;
    }

    @Override
    public void run() {
        while (!this.isStopped()) {
            //等待一秒 计算一会秒内的传输字节
            this.waitForRunning(1 * 1000);
            this.calculateSpeed();
        }
    }
    /**
     * 将传输字节设置 一秒内传输字节 重置传输字节
     */
    public void calculateSpeed() {
        this.transferredByteInSecond = this.transferredByte.get();
        this.transferredByte.set(0);
    }

    public int canTransferMaxByteNum() {
        //Flow control is not started at present
        //是否启用流量控制
        //计算剩余可传输 字节数量
        if (this.isFlowControlEnable()) {
            long res = Math.max(this.maxTransferByteInSecond() - this.transferredByte.get(), 0);
            return res > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) res;
        }
        return Integer.MAX_VALUE;
    }
    /**
     * 添加已经传输的字节数
     */
    public void addByteCountTransferred(long count) {
        this.transferredByte.addAndGet(count);
    }

    public long getTransferredByteInSecond() {
        return this.transferredByteInSecond;
    }

    @Override
    public String getServiceName() {
        return FlowMonitor.class.getSimpleName();
    }

    protected boolean isFlowControlEnable() {
        return this.messageStoreConfig.isHaFlowControlEnable();
    }

    public long maxTransferByteInSecond() {
        return this.messageStoreConfig.getMaxHaTransferByteInSecond();
    }
}