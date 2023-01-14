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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.AbstractBrokerRunnable;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * 消费客户端发生改变监听
 */
public class DefaultConsumerIdsChangeListener implements ConsumerIdsChangeListener {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;
    /**
     * 缓存大小
     */
    private final int cacheSize = 8096;
    /**
     * 定时处理 consumerChannelMap  消费者发生改变监听 调用
     */
    private final ScheduledExecutorService scheduledExecutorService =  new ScheduledThreadPoolExecutor(1,
        ThreadUtils.newGenericThreadFactory("DefaultConsumerIdsChangeListener", true));
    /**
     * key 为 消费组 ,value 为 发生改变的 消费组客户端 发生改变的缓存
     */
    private ConcurrentHashMap<String,List<Channel>> consumerChannelMap = new ConcurrentHashMap<>(cacheSize);

    public DefaultConsumerIdsChangeListener(BrokerController brokerController) {
        this.brokerController = brokerController;

        scheduledExecutorService.scheduleAtFixedRate(new AbstractBrokerRunnable(brokerController.getBrokerConfig()) {
            @Override
            public void run2() {
                try {
                    notifyConsumerChange();
                } catch (Exception e) {
                    log.error(
                        "DefaultConsumerIdsChangeListen#notifyConsumerChange: unexpected error occurs", e);
                }
            }
        }, 30, 15, TimeUnit.SECONDS);
    }

    @Override
    public void handle(ConsumerGroupEvent event, String group, Object... args) {
        if (event == null) {
            return;
        }
        switch (event) {
            case CHANGE:
                if (args == null || args.length < 1) {
                    return;
                }
                List<Channel> channels = (List<Channel>) args[0];
                if (channels != null && brokerController.getBrokerConfig().isNotifyConsumerIdsChangedEnable()) {
                    //事实通知消费者发生改变
                    if (this.brokerController.getBrokerConfig().isRealTimeNotifyConsumerChange()) {
                        for (Channel chl : channels) {
                            this.brokerController.getBroker2Client().notifyConsumerIdsChanged(chl, group);
                        }
                    } else {
                        //不是事实的则 添加到 consumerChannelMap 定时通知
                        consumerChannelMap.put(group, channels);
                    }
                }
                break;
            case UNREGISTER:
                //消费过滤 取消该消息组的注册
                this.brokerController.getConsumerFilterManager().unRegister(group);
                break;
            case REGISTER:
                if (args == null || args.length < 1) {
                    return;
                }
                //消费过滤 注册该消息组的
                Collection<SubscriptionData> subscriptionDataList = (Collection<SubscriptionData>) args[0];
                this.brokerController.getConsumerFilterManager().register(group, subscriptionDataList);
                break;
            case CLIENT_REGISTER:
            case CLIENT_UNREGISTER:
                break;
            default:
                throw new RuntimeException("Unknown event " + event);
        }
    }

    /**
     * 通知客户端消费发生改变
     */
    private void notifyConsumerChange() {

        if (consumerChannelMap.isEmpty()) {
            return;
        }

        ConcurrentHashMap<String, List<Channel>> processMap = new ConcurrentHashMap<>(consumerChannelMap);
        consumerChannelMap = new ConcurrentHashMap<>(cacheSize);
        //遍历消费者 客户端发生改变客户端 通知客户端发生改变
        for (Map.Entry<String, List<Channel>> entry : processMap.entrySet()) {
            String consumerId = entry.getKey();
            List<Channel> channelList = entry.getValue();
            try {
                if (channelList != null && brokerController.getBrokerConfig().isNotifyConsumerIdsChangedEnable()) {
                    for (Channel chl : channelList) {
                        this.brokerController.getBroker2Client().notifyConsumerIdsChanged(chl, consumerId);
                    }
                }
            } catch (Exception e) {
                log.error("Failed to notify consumer when some consumers changed, consumerId to notify: {}",
                    consumerId, e);
            }
        }
    }

    @Override
    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }
}
