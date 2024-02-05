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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAConnection;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;

/**
 * GroupTransferService Service
 */
public class GroupTransferService extends ServiceThread {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
    /**
     * 写队列
     */
    private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new ArrayList<>();
    /**
     * 读队列
     */
    private volatile List<CommitLog.GroupCommitRequest> requestsRead = new ArrayList<>();
    private HAService haService;
    private DefaultMessageStore defaultMessageStore;

    public GroupTransferService(final HAService haService, final DefaultMessageStore defaultMessageStore) {
        this.haService = haService;
        this.defaultMessageStore = defaultMessageStore;
    }

    /**
     * 将请求 添加到写队列当中 标志通知标记 减少等待点
     * @param request
     */
    public synchronized void putRequest(final CommitLog.GroupCommitRequest request) {
        synchronized (this.requestsWrite) {
            this.requestsWrite.add(request);
        }
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
    }

    public void notifyTransferSome() {
        this.notifyTransferObject.wakeup();
    }

    /**
     * 将读写队列进行交换
     */
    private void swapRequests() {
        List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
        this.requestsWrite = this.requestsRead;
        this.requestsRead = tmp;
    }

    private void doWaitTransfer() {
        //对读队列进行上锁 遍历读队列
        synchronized (this.requestsRead) {
            if (!this.requestsRead.isEmpty()) {
                for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                    boolean transferOK = false;
                    //获取请求 的截止时间
                    long deadLine = req.getDeadLine();
                    // 需要 所有 同步 状态 ack 确认
                    final boolean allAckInSyncStateSet = req.getAckNums() == MixAll.ALL_ACK_IN_SYNC_STATE_SET;

                    for (int i = 0; !transferOK && deadLine - System.nanoTime() > 0; i++) {
                        //超过一次 传输失败 则进行等待 1 s
                        if (i > 0) {
                            this.notifyTransferObject.waitForRunning(1000);
                        }
                        //不需要 所有 同步 状态 ack 确认 并且 ack偏移量 小于等于 1 包含 master 自己 则 只要 master 推送 的偏移量 大于请求的偏移量就行
                        if (!allAckInSyncStateSet && req.getAckNums() <= 1) {
                            transferOK = haService.getPush2SlaveMaxOffset().get() >= req.getNextOffset();
                            continue;
                        }

                        if (allAckInSyncStateSet && this.haService instanceof AutoSwitchHAService) {
                            // In this mode, we must wait for all replicas that in InSyncStateSet.
                            // 等待所有 同步状态 的 replicas
                            final AutoSwitchHAService autoSwitchHAService = (AutoSwitchHAService) this.haService;
                            final Set<String> syncStateSet = autoSwitchHAService.getSyncStateSet();
                            //没有同步状态的集合 则只有 master
                            if (syncStateSet.size() <= 1) {
                                // Only master
                                transferOK = true;
                                break;
                            }

                            // Include master
                            // 包含 master 遍历 salve 判断同步的集合 根据偏移量 进行判断 是否确认
                            int ackNums = 1;
                            for (HAConnection conn : haService.getConnectionList()) {
                                final AutoSwitchHAConnection autoSwitchHAConnection = (AutoSwitchHAConnection) conn;
                                if (syncStateSet.contains(autoSwitchHAConnection.getSlaveAddress()) && autoSwitchHAConnection.getSlaveAckOffset() >= req.getNextOffset()) {
                                    ackNums++;
                                }
                                //确认的数量 超过 同步的 数量表示传输 成功
                                if (ackNums >= syncStateSet.size()) {
                                    transferOK = true;
                                    break;
                                }
                            }
                        } else {
                            // Include master
                            //ack数量包含 master
                            int ackNums = 1;
                            //遍历salve获取 确认偏移量 超过请求的偏移量 说明已经去人
                            for (HAConnection conn : haService.getConnectionList()) {
                                // TODO: We must ensure every HAConnection represents a different slave
                                // Solution: Consider assign a unique and fixed IP:ADDR for each different slave
                                if (conn.getSlaveAckOffset() >= req.getNextOffset()) {
                                    ackNums++;
                                }
                                //如果 确认的数量 超过请求的 确认的数量 表示 传输成功
                                if (ackNums >= req.getAckNums()) {
                                    transferOK = true;
                                    break;
                                }
                            }
                        }
                    }
                    //传输 是否成功
                    if (!transferOK) {
                        log.warn("transfer message to slave timeout, offset : {}, request acks: {}",
                            req.getNextOffset(), req.getAckNums());
                    }
                    // 进行唤醒
                    req.wakeupCustomer(transferOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                }
                //清空读 队列
                this.requestsRead.clear();
            }
        }
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.waitForRunning(10);
                //进行传输
                this.doWaitTransfer();
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    protected void onWaitEnd() {
        //交换读写队列
        this.swapRequests();
    }

    @Override
    public String getServiceName() {
        if (defaultMessageStore != null && defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
            return defaultMessageStore.getBrokerIdentity().getLoggerIdentifier() + GroupTransferService.class.getSimpleName();
        }
        return GroupTransferService.class.getSimpleName();
    }
}
