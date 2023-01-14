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
package org.apache.rocketmq.namesrv.route;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * 处理获取路由信息请求的响应 过滤出对应的 zone 下面的路由信息
 */
public class ZoneRouteRPCHook implements RPCHook {

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {

    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
        if (RequestCode.GET_ROUTEINFO_BY_TOPIC != request.getCode()) {
            return;
        }
        if (response == null || response.getBody() == null || ResponseCode.SUCCESS != response.getCode()) {
            return;
        }
        //从请求当中获取 zoneMode
        boolean zoneMode = Boolean.valueOf(request.getExtFields().get(MixAll.ZONE_MODE));
        if (!zoneMode) {
            return;
        }
        //从请求当中获取 ZONE_NAME
        String zoneName = request.getExtFields().get(MixAll.ZONE_NAME);
        if (StringUtils.isBlank(zoneName)) {
            return;
        }
        //解码成 TopicRouteData
        TopicRouteData topicRouteData = RemotingSerializable.decode(response.getBody(), TopicRouteData.class);

        response.setBody(filterByZoneName(topicRouteData, zoneName).encode());
    }

    /**
     * 过滤出对应的 zone 下面的路由信息
     * @param topicRouteData
     * @param zoneName
     * @return
     */
    private TopicRouteData filterByZoneName(TopicRouteData topicRouteData, String zoneName) {
        //记录没有主的 broker 和  zoneName 下的 broker 要被保留的
        List<BrokerData> brokerDataReserved = new ArrayList<>();
        //记录存在 主 broker 或者 不是  zoneName 下的 broker 要被移除
        Map<String, BrokerData> brokerDataRemoved = new HashMap<>();
        //遍历 BrokerData
        for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
            //master down, consume from slave. break nearby route rule.
            //FIXME:: 没有主broker 是为干啥
            //没有 主broker 主 broker 已经宕机 则设置 zoneName 下的 broker
            if (brokerData.getBrokerAddrs().get(MixAll.MASTER_ID) == null
                || StringUtils.equalsIgnoreCase(brokerData.getZoneName(), zoneName)) {
                brokerDataReserved.add(brokerData);
            } else {
                //存在 主 broker 或者 不是  zoneName 下的 broker
                brokerDataRemoved.put(brokerData.getBrokerName(), brokerData);
            }
        }
        topicRouteData.setBrokerDatas(brokerDataReserved);
        //需要被保留的QueueData
        List<QueueData> queueDataReserved = new ArrayList<>();
        //遍历队列  QueueData  记录没有在 被 移除broker 下的 队列
        for (QueueData queueData : topicRouteData.getQueueDatas()) {
            if (!brokerDataRemoved.containsKey(queueData.getBrokerName())) {
                queueDataReserved.add(queueData);
            }
        }
        //设置队列信息 去掉被移除的 broker   filterServer
        topicRouteData.setQueueDatas(queueDataReserved);
        // remove filter server table by broker address
        if (topicRouteData.getFilterServerTable() != null && !topicRouteData.getFilterServerTable().isEmpty()) {
            for (Entry<String, BrokerData> entry : brokerDataRemoved.entrySet()) {
                BrokerData brokerData = entry.getValue();
                if (brokerData.getBrokerAddrs() == null) {
                    continue;
                }
                brokerData.getBrokerAddrs().values()
                    .stream()
                    .forEach(brokerAddr -> topicRouteData.getFilterServerTable().remove(brokerAddr));
            }
        }
        return topicRouteData;
    }
}
