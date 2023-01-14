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
package org.apache.rocketmq.client.impl.consumer;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.message.MessageQueue;

public class AssignedMessageQueue {

    private final ConcurrentHashMap<MessageQueue, MessageQueueState> assignedMessageQueueState;

    private RebalanceImpl rebalanceImpl;

    public AssignedMessageQueue() {
        assignedMessageQueueState = new ConcurrentHashMap<MessageQueue, MessageQueueState>();
    }

    public void setRebalanceImpl(RebalanceImpl rebalanceImpl) {
        this.rebalanceImpl = rebalanceImpl;
    }

    public Set<MessageQueue> messageQueues() {
        return assignedMessageQueueState.keySet();
    }

    /**
     * 判断该消息队列是否暂停
     * @param messageQueue
     * @return
     */
    public boolean isPaused(MessageQueue messageQueue) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            return messageQueueState.isPaused();
        }
        return true;
    }
    /**
     * 将 该消息队列集合 的消息状态 暂停
     * @param messageQueues
     */
    public void pause(Collection<MessageQueue> messageQueues) {
        for (MessageQueue messageQueue : messageQueues) {
            MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
            if (assignedMessageQueueState.get(messageQueue) != null) {
                messageQueueState.setPaused(true);
            }
        }
    }

    /**
     * 将 该消息队列集合 的消息状态 重新恢复
     * @param messageQueueCollection
     */
    public void resume(Collection<MessageQueue> messageQueueCollection) {
        for (MessageQueue messageQueue : messageQueueCollection) {
            MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
            if (assignedMessageQueueState.get(messageQueue) != null) {
                messageQueueState.setPaused(false);
            }
        }
    }

    /**
     * 获取处理队列
     * @param messageQueue
     * @return
     */
    public ProcessQueue getProcessQueue(MessageQueue messageQueue) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            return messageQueueState.getProcessQueue();
        }
        return null;
    }
    /**
     * 获取消息队列获取消息偏移量
     * @param messageQueue
     */
    public long getPullOffset(MessageQueue messageQueue) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            return messageQueueState.getPullOffset();
        }
        return -1;
    }

    /**
     * 更新消息队列获取消息偏移量
     * @param messageQueue
     * @param offset
     * @param processQueue
     */
    public void updatePullOffset(MessageQueue messageQueue, long offset, ProcessQueue processQueue) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            if (messageQueueState.getProcessQueue() != processQueue) {
                return;
            }
            messageQueueState.setPullOffset(offset);
        }
    }

    /**
     * 获取消息队列 消费偏移量
     * @param messageQueue
     * @return
     */
    public long getConsumerOffset(MessageQueue messageQueue) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            return messageQueueState.getConsumeOffset();
        }
        return -1;
    }

    /**
     * 更新消费队列的消费偏移量
     * @param messageQueue
     * @param offset
     */
    public void updateConsumeOffset(MessageQueue messageQueue, long offset) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            messageQueueState.setConsumeOffset(offset);
        }
    }

    public void setSeekOffset(MessageQueue messageQueue, long offset) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            messageQueueState.setSeekOffset(offset);
        }
    }

    public long getSeekOffset(MessageQueue messageQueue) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            return messageQueueState.getSeekOffset();
        }
        return -1;
    }

    /**
     * 遍历 assignedMessageQueueState 将 topic 下 不在 assigned 标记丢弃 进行移除
     * 将 assigned 中 消息队列 添加 到 assignedMessageQueueState
     * @param topic
     * @param assigned
     */
    public void updateAssignedMessageQueue(String topic, Collection<MessageQueue> assigned) {
        //遍历 assignedMessageQueueState 将 topic 下 不在 assigned 标记丢弃 进行移除
        synchronized (this.assignedMessageQueueState) {
            Iterator<Map.Entry<MessageQueue, MessageQueueState>> it = this.assignedMessageQueueState.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<MessageQueue, MessageQueueState> next = it.next();
                if (next.getKey().getTopic().equals(topic)) {
                    if (!assigned.contains(next.getKey())) {
                        next.getValue().getProcessQueue().setDropped(true);
                        it.remove();
                    }
                }
            }
            //将 assigned 中 消息队列 添加 到 assignedMessageQueueState
            addAssignedMessageQueue(assigned);
        }
    }
    /**
     * 遍历 assignedMessageQueueState  不在 assigned 标记丢弃 进行移除
     * 将 assigned 中 消息队列 添加 到 assignedMessageQueueState
     * @param assigned
     */
    public void updateAssignedMessageQueue(Collection<MessageQueue> assigned) {
        synchronized (this.assignedMessageQueueState) {
            Iterator<Map.Entry<MessageQueue, MessageQueueState>> it = this.assignedMessageQueueState.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<MessageQueue, MessageQueueState> next = it.next();
                if (!assigned.contains(next.getKey())) {
                    next.getValue().getProcessQueue().setDropped(true);
                    it.remove();
                }
            }
            addAssignedMessageQueue(assigned);
        }
    }

    /**
     * 遍历 assigned 将 消息队列 添加 到 assignedMessageQueueState 中
     * @param assigned
     */
    private void addAssignedMessageQueue(Collection<MessageQueue> assigned) {
        //遍历 assigned 将 消息队列 添加 到 assignedMessageQueueState 中
        for (MessageQueue messageQueue : assigned) {
            if (!this.assignedMessageQueueState.containsKey(messageQueue)) {
                MessageQueueState messageQueueState;
                if (rebalanceImpl != null && rebalanceImpl.getProcessQueueTable().get(messageQueue) != null) {
                    messageQueueState = new MessageQueueState(messageQueue, rebalanceImpl.getProcessQueueTable().get(messageQueue));
                } else {
                    ProcessQueue processQueue = new ProcessQueue();
                    messageQueueState = new MessageQueueState(messageQueue, processQueue);
                }
                this.assignedMessageQueueState.put(messageQueue, messageQueueState);
            }
        }
    }

    /**
     * 遍历 assignedMessageQueueState 移除 该 topic 下的消息 队列
     * @param topic
     */
    public void removeAssignedMessageQueue(String topic) {
        synchronized (this.assignedMessageQueueState) {
            Iterator<Map.Entry<MessageQueue, MessageQueueState>> it = this.assignedMessageQueueState.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<MessageQueue, MessageQueueState> next = it.next();
                if (next.getKey().getTopic().equals(topic)) {
                    it.remove();
                }
            }
        }
    }

    public Set<MessageQueue> getAssignedMessageQueues() {
        return this.assignedMessageQueueState.keySet();
    }

    private class MessageQueueState {
        /**
         * 消息队列
         */
        private MessageQueue messageQueue;
        /**
         * 处理队列
         */
        private ProcessQueue processQueue;
        /**
         * 暂停标志
         */
        private volatile boolean paused = false;
        /**
         * 获取消息偏移量
         */
        private volatile long pullOffset = -1;
        /**
         * 消费偏移量
         */
        private volatile long consumeOffset = -1;
        private volatile long seekOffset = -1;

        private MessageQueueState(MessageQueue messageQueue, ProcessQueue processQueue) {
            this.messageQueue = messageQueue;
            this.processQueue = processQueue;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        public void setMessageQueue(MessageQueue messageQueue) {
            this.messageQueue = messageQueue;
        }

        public boolean isPaused() {
            return paused;
        }

        public void setPaused(boolean paused) {
            this.paused = paused;
        }

        public long getPullOffset() {
            return pullOffset;
        }

        public void setPullOffset(long pullOffset) {
            this.pullOffset = pullOffset;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        public void setProcessQueue(ProcessQueue processQueue) {
            this.processQueue = processQueue;
        }

        public long getConsumeOffset() {
            return consumeOffset;
        }

        public void setConsumeOffset(long consumeOffset) {
            this.consumeOffset = consumeOffset;
        }

        public long getSeekOffset() {
            return seekOffset;
        }

        public void setSeekOffset(long seekOffset) {
            this.seekOffset = seekOffset;
        }
    }
}
