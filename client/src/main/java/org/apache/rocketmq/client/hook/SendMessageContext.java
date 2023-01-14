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
package org.apache.rocketmq.client.hook;

import java.util.Map;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageType;

/**
 * 发送消息的Context
 */
public class SendMessageContext {
    /**
     * 生产组
     */
    private String producerGroup;
    /**
     * 发送的消息
     */
    private Message message;
    /**
     * 发送的消息队列
     */
    private MessageQueue mq;
    /**
     * broker地址
     */
    private String brokerAddr;
    /**
     * 生产者生成消息的host
     */
    private String bornHost;
    /**
     * 发送消息的方式 同步,异步,单向
     */
    private CommunicationMode communicationMode;
    /**
     * 发送消息后的结果
     */
    private SendResult sendResult;
    private Exception exception;
    /**
     * 跟踪 context
     */
    private Object mqTraceContext;
    private Map<String, String> props;
    /**
     * 生产者实例
     */
    private DefaultMQProducerImpl producer;
    private MessageType msgType = MessageType.Normal_Msg;
    /**
     * 生产者的namespace
     */
    private String namespace;

    public MessageType getMsgType() {
        return msgType;
    }

    public void setMsgType(final MessageType msgType) {
        this.msgType = msgType;
    }

    public DefaultMQProducerImpl getProducer() {
        return producer;
    }

    public void setProducer(final DefaultMQProducerImpl producer) {
        this.producer = producer;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public MessageQueue getMq() {
        return mq;
    }

    public void setMq(MessageQueue mq) {
        this.mq = mq;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public CommunicationMode getCommunicationMode() {
        return communicationMode;
    }

    public void setCommunicationMode(CommunicationMode communicationMode) {
        this.communicationMode = communicationMode;
    }

    public SendResult getSendResult() {
        return sendResult;
    }

    public void setSendResult(SendResult sendResult) {
        this.sendResult = sendResult;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public Object getMqTraceContext() {
        return mqTraceContext;
    }

    public void setMqTraceContext(Object mqTraceContext) {
        this.mqTraceContext = mqTraceContext;
    }

    public Map<String, String> getProps() {
        return props;
    }

    public void setProps(Map<String, String> props) {
        this.props = props;
    }

    public String getBornHost() {
        return bornHost;
    }

    public void setBornHost(String bornHost) {
        this.bornHost = bornHost;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
}
