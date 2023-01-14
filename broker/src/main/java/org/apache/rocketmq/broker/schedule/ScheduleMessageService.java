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
package org.apache.rocketmq.broker.schedule;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.ReferredIterator;

/**
 * 消息调度
 */
public class ScheduleMessageService extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * 第一延迟时间
     */
    private static final long FIRST_DELAY_TIME = 1000L;
    /**
     * 延迟一段时间
     */
    private static final long DELAY_FOR_A_WHILE = 100L;
    private static final long DELAY_FOR_A_PERIOD = 10000L;
    private static final long WAIT_FOR_SHUTDOWN = 5000L;
    private static final long DELAY_FOR_A_SLEEP = 10L;
    /**
     * key  为 延迟 等级 ,  value 为延迟 时间
     */
    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
        new ConcurrentHashMap<Integer, Long>(32);
    /**
     * key 为延迟 等级 , value 为 该等级的偏移量
     */
    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
        new ConcurrentHashMap<Integer, Long>(32);
    /**
     * 启动状态
     */
    private final AtomicBoolean started = new AtomicBoolean(false);
    /**
     * 分发线程池
     */
    private ScheduledExecutorService deliverExecutorService;
    /**
     * 最大延迟 等级 作为分发 线程池的 核心 线程数量
     */
    private int maxDelayLevel;
    /**
     * 版本号
     */
    private DataVersion dataVersion = new DataVersion();
    /**
     * 是否异步 Deliver
     */
    private boolean enableAsyncDeliver = false;
    /**
     * 如果采用 异步 enableAsyncDeliver
     */
    private ScheduledExecutorService handleExecutorService;
    /***
     * 定时进行 持久化
     */
    private final ScheduledExecutorService scheduledPersistService;
    /**
     * key 为延迟 等级
     */
    private final Map<Integer /* level */, LinkedBlockingQueue<PutResultProcess>> deliverPendingTable =
        new ConcurrentHashMap<>(32);
    private final BrokerController brokerController;
    private final transient AtomicLong versionChangeCounter = new AtomicLong(0);

    public ScheduleMessageService(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.enableAsyncDeliver = brokerController.getMessageStoreConfig().isEnableScheduleAsyncDeliver();
        scheduledPersistService = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryImpl("ScheduleMessageServicePersistThread", true, brokerController.getBrokerConfig()));
        //持久化
        scheduledPersistService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    ScheduleMessageService.this.persist();
                } catch (Throwable e) {
                    log.error("scheduleAtFixedRate flush exception", e);
                }
            }
        }, 10000, this.brokerController.getMessageStoreConfig().getFlushDelayOffsetInterval(), TimeUnit.MILLISECONDS);
    }

    /**
     * 消息队列 id 转成 延迟等级 + 1
     * @param queueId
     * @return
     */
    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    /**
     * 延迟等级 转成消息队列id  -1
     * @param delayLevel
     * @return
     */
    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }

    public void buildRunningStats(HashMap<String, String> stats) {
        //遍历 offsetTable 延迟等级 的偏移量
        Iterator<Map.Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Long> next = it.next();
            //延迟等级转成 消息队列 Id
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            //获取 SCHEDULE_TOPIC_XXXX  topic  消息队列 id 为 queueId 最大偏移量
            long maxOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    private void updateOffset(int delayLevel, long offset) {
        //更新延迟等级的偏移量 如果版本
        this.offsetTable.put(delayLevel, offset);
        //版本计数为延迟等级偏移量更新步调的倍数 则更新 版本
        if (versionChangeCounter.incrementAndGet() % brokerController.getBrokerConfig().getDelayOffsetUpdateVersionStep() == 0) {
            long stateMachineVersion = brokerController.getMessageStore() != null ? brokerController.getMessageStore().getStateMachineVersion() : 0;
            dataVersion.nextVersion(stateMachineVersion);
        }
    }

    /**
     * 计算分发时间
     * @param delayLevel
     * @param storeTimestamp
     * @return
     */
    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        //根据延迟等级 来获取延迟 时间 + 消息存储 时间 为分发时间
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            this.load();
            //初始化分发线程池
            this.deliverExecutorService = new ScheduledThreadPoolExecutor(this.maxDelayLevel, new ThreadFactoryImpl("ScheduleMessageTimerThread_"));
            //异步线程池
            if (this.enableAsyncDeliver) {
                this.handleExecutorService = new ScheduledThreadPoolExecutor(this.maxDelayLevel, new ThreadFactoryImpl("ScheduleMessageExecutorHandleThread_"));
            }
            //遍历 delayLevelTable  延迟等级 对应 的 延迟时间表
            for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
                Integer level = entry.getKey();
                Long timeDelay = entry.getValue();
                //获取该延迟的等级的偏移量
                Long offset = this.offsetTable.get(level);
                if (null == offset) {
                    offset = 0L;
                }

                if (timeDelay != null) {
                    if (this.enableAsyncDeliver) {
                        this.handleExecutorService.schedule(new HandlePutResultTask(level), FIRST_DELAY_TIME, TimeUnit.MILLISECONDS);
                    }
                    this.deliverExecutorService.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME, TimeUnit.MILLISECONDS);
                }
            }
            //进行持久化
            this.deliverExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        if (started.get()) {
                            ScheduleMessageService.this.persist();
                        }
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate flush exception", e);
                    }
                }
            }, 10000, this.brokerController.getMessageStore().getMessageStoreConfig().getFlushDelayOffsetInterval(), TimeUnit.MILLISECONDS);
        }
    }

    public void shutdown() {
        stop();
        ThreadUtils.shutdown(scheduledPersistService);
    }

    public void stop() {
        if (this.started.compareAndSet(true, false) && null != this.deliverExecutorService) {
            this.deliverExecutorService.shutdown();
            try {
                this.deliverExecutorService.awaitTermination(WAIT_FOR_SHUTDOWN, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.error("deliverExecutorService awaitTermination error", e);
            }

            if (this.handleExecutorService != null) {
                this.handleExecutorService.shutdown();
                try {
                    this.handleExecutorService.awaitTermination(WAIT_FOR_SHUTDOWN, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    log.error("handleExecutorService awaitTermination error", e);
                }
            }

            if (this.deliverPendingTable != null) {
                for (int i = 1; i <= this.deliverPendingTable.size(); i++) {
                    log.warn("deliverPendingTable level: {}, size: {}", i, this.deliverPendingTable.get(i).size());
                }
            }

            this.persist();
        }
    }

    public boolean isStarted() {
        return started.get();
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public boolean load() {
        boolean result = super.load();
        result = result && this.parseDelayLevel();
        result = result && this.correctDelayOffset();
        return result;
    }

    public boolean correctDelayOffset() {
        try {
            //遍历 delayLevelTable
            for (int delayLevel : delayLevelTable.keySet()) {
                //获取 SCHEDULE_TOPIC_XXXX 对应队列的 消费消息队列 接口
                ConsumeQueueInterface cq =
                    brokerController.getMessageStore().getQueueStore().findOrCreateConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
                        delayLevel2QueueId(delayLevel));
                Long currentDelayOffset = offsetTable.get(delayLevel);
                if (currentDelayOffset == null || cq == null) {
                    continue;
                }
                //当前 延迟等级的 偏移量
                long correctDelayOffset = currentDelayOffset;
                //获取 该消费队列的 最小偏移量 和 最大偏移量
                long cqMinOffset = cq.getMinOffsetInQueue();
                long cqMaxOffset = cq.getMaxOffsetInQueue();
                //小于该队列最小偏移量 说 延迟等级偏移量无效
                if (currentDelayOffset < cqMinOffset) {
                    correctDelayOffset = cqMinOffset;
                    log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, queueId={}",
                        currentDelayOffset, cqMinOffset, cqMaxOffset, cq.getQueueId());
                }
                //大于该队列最大偏移量 说 延迟等级偏移量无效
                if (currentDelayOffset > cqMaxOffset) {
                    correctDelayOffset = cqMaxOffset;
                    log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, queueId={}",
                        currentDelayOffset, cqMinOffset, cqMaxOffset, cq.getQueueId());
                }
                //更新延迟等级的偏移量
                if (correctDelayOffset != currentDelayOffset) {
                    log.error("correct delay offset [ delayLevel {} ] from {} to {}", delayLevel, currentDelayOffset, correctDelayOffset);
                    offsetTable.put(delayLevel, correctDelayOffset);
                }
            }
        } catch (Exception e) {
            log.error("correctDelayOffset exception", e);
            return false;
        }
        return true;
    }

    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getDelayOffsetStorePath(this.brokerController.getMessageStore().getMessageStoreConfig()
            .getStorePathRootDir());
    }

    /**
     * load 时间 加载配置文件 根据文件获取 offsetTable
     * @param jsonString
     */
    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
                // For compatible
                if (delayOffsetSerializeWrapper.getDataVersion() != null) {
                    this.dataVersion.assignNewOne(delayOffsetSerializeWrapper.getDataVersion());
                }
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        delayOffsetSerializeWrapper.setDataVersion(this.dataVersion);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    /**
     * 生成 delayLevelTable 延迟等级 对应 延迟时间 映射
     * @return
     */
    public boolean parseDelayLevel() {
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);
        // "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"
        String levelString = this.brokerController.getMessageStoreConfig().getMessageDelayLevel();
        try {
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                //单位
                String ch = value.substring(value.length() - 1);
                //获取单位的 时间 毫秒
                Long tu = timeUnitTable.get(ch);

                int level = i + 1;
                //记录最大的延迟等级
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }
                //数量
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                //计算延迟时间
                long delayTimeMillis = tu * num;
                //进行延迟等级的映射
                this.delayLevelTable.put(level, delayTimeMillis);
                //异步分发初始  deliverPendingTable 表
                if (this.enableAsyncDeliver) {
                    this.deliverPendingTable.put(level, new LinkedBlockingQueue<>());
                }
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }

    private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());

        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue =
            MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

        msgInner.setWaitStoreMsgOK(false);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_TIMER_DELIVER_MS);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_TIMER_DELAY_SEC);

        msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

        String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
        int queueId = Integer.parseInt(queueIdStr);
        msgInner.setQueueId(queueId);

        return msgInner;
    }

    /**
     * 根据时间计算计算 delayLevel
     * @param timeMillis
     * @return
     */
    public int computeDelayLevel(long timeMillis) {
        long intervalMillis = timeMillis - System.currentTimeMillis();
        List<Map.Entry<Integer, Long>> sortedLevels = delayLevelTable.entrySet().stream().sorted(Comparator.comparingLong(Map.Entry::getValue)).collect(Collectors.toList());
        for (Map.Entry<Integer, Long> entry : sortedLevels) {
            if (entry.getValue() > intervalMillis) {
                return entry.getKey();
            }
        }
        return sortedLevels.get(sortedLevels.size() - 1).getKey();
    }

    class DeliverDelayedMessageTimerTask implements Runnable {
        /**
         * 延迟等级
         */
        private final int delayLevel;
        /**
         * 偏移量
         */
        private final long offset;

        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }

        @Override
        public void run() {
            try {
                if (isStarted()) {
                    this.executeOnTimeup();
                }
            } catch (Exception e) {
                // XXX: warn and notify me
                log.error("ScheduleMessageService, executeOnTimeup exception", e);
                this.scheduleNextTimerTask(this.offset, DELAY_FOR_A_PERIOD);
            }
        }

        /**
         * @return
         */
        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {

            long result = deliverTimestamp;

            long maxTimestamp = now + ScheduleMessageService.this.delayLevelTable.get(this.delayLevel);
            if (deliverTimestamp > maxTimestamp) {
                result = now;
            }

            return result;
        }

        public void executeOnTimeup() {
            //获取 SCHEDULE_TOPIC_XXXX 对应队列的 消费消息队列 接口
            ConsumeQueueInterface cq =
                ScheduleMessageService.this.brokerController.getMessageStore().getConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
                    delayLevel2QueueId(delayLevel));

            if (cq == null) {
                this.scheduleNextTimerTask(this.offset, DELAY_FOR_A_WHILE);
                return;
            }

            ReferredIterator<CqUnit> bufferCQ = cq.iterateFrom(this.offset);
            if (bufferCQ == null) {
                long resetOffset;
                if ((resetOffset = cq.getMinOffsetInQueue()) > this.offset) {
                    log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, queueId={}",
                        this.offset, resetOffset, cq.getQueueId());
                } else if ((resetOffset = cq.getMaxOffsetInQueue()) < this.offset) {
                    log.error("schedule CQ offset invalid. offset={}, cqMaxOffset={}, queueId={}",
                        this.offset, resetOffset, cq.getQueueId());
                } else {
                    resetOffset = this.offset;
                }
                //重新进行调度
                this.scheduleNextTimerTask(resetOffset, DELAY_FOR_A_WHILE);
                return;
            }

            long nextOffset = this.offset;
            try {
                while (bufferCQ.hasNext() && isStarted()) {
                    CqUnit cqUnit = bufferCQ.next();
                    long offsetPy = cqUnit.getPos();
                    int sizePy = cqUnit.getSize();
                    long tagsCode = cqUnit.getTagsCode();

                    if (!cqUnit.isTagsCodeValid()) {
                        //can't find ext content.So re compute tags code.
                        log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}",
                            tagsCode, offsetPy, sizePy);
                        long msgStoreTime = ScheduleMessageService.this.brokerController.getMessageStore().getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                        tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                    }

                    long now = System.currentTimeMillis();
                    long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);

                    long currOffset = cqUnit.getQueueOffset();
                    assert cqUnit.getBatchNum() == 1;
                    nextOffset = currOffset + cqUnit.getBatchNum();

                    long countdown = deliverTimestamp - now;
                    if (countdown > 0) {
                        this.scheduleNextTimerTask(currOffset, DELAY_FOR_A_WHILE);
                        ScheduleMessageService.this.updateOffset(this.delayLevel, currOffset);
                        return;
                    }

                    MessageExt msgExt = ScheduleMessageService.this.brokerController.getMessageStore().lookMessageByOffset(offsetPy, sizePy);
                    if (msgExt == null) {
                        continue;
                    }

                    MessageExtBrokerInner msgInner = ScheduleMessageService.this.messageTimeup(msgExt);
                    if (TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC.equals(msgInner.getTopic())) {
                        log.error("[BUG] the real topic of schedule msg is {}, discard the msg. msg={}",
                            msgInner.getTopic(), msgInner);
                        continue;
                    }

                    boolean deliverSuc;
                    if (ScheduleMessageService.this.enableAsyncDeliver) {
                        deliverSuc = this.asyncDeliver(msgInner, msgExt.getMsgId(), currOffset, offsetPy, sizePy);
                    } else {
                        deliverSuc = this.syncDeliver(msgInner, msgExt.getMsgId(), currOffset, offsetPy, sizePy);
                    }

                    if (!deliverSuc) {
                        this.scheduleNextTimerTask(nextOffset, DELAY_FOR_A_WHILE);
                        return;
                    }
                }
            } catch (Exception e) {
                log.error("ScheduleMessageService, messageTimeup execute error, offset = {}", nextOffset, e);
            } finally {
                bufferCQ.release();
            }

            this.scheduleNextTimerTask(nextOffset, DELAY_FOR_A_WHILE);
        }

        public void scheduleNextTimerTask(long offset, long delay) {
            ScheduleMessageService.this.deliverExecutorService.schedule(new DeliverDelayedMessageTimerTask(
                this.delayLevel, offset), delay, TimeUnit.MILLISECONDS);
        }

        private boolean syncDeliver(MessageExtBrokerInner msgInner, String msgId, long offset, long offsetPy,
            int sizePy) {
            PutResultProcess resultProcess = deliverMessage(msgInner, msgId, offset, offsetPy, sizePy, false);
            PutMessageResult result = resultProcess.get();
            boolean sendStatus = result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK;
            if (sendStatus) {
                ScheduleMessageService.this.updateOffset(this.delayLevel, resultProcess.getNextOffset());
            }
            return sendStatus;
        }

        private boolean asyncDeliver(MessageExtBrokerInner msgInner, String msgId, long offset, long offsetPy,
            int sizePy) {
            Queue<PutResultProcess> processesQueue = ScheduleMessageService.this.deliverPendingTable.get(this.delayLevel);

            //Flow Control
            int currentPendingNum = processesQueue.size();
            int maxPendingLimit = brokerController.getMessageStoreConfig()
                .getScheduleAsyncDeliverMaxPendingLimit();
            if (currentPendingNum > maxPendingLimit) {
                log.warn("Asynchronous deliver triggers flow control, " +
                    "currentPendingNum={}, maxPendingLimit={}", currentPendingNum, maxPendingLimit);
                return false;
            }

            //Blocked
            PutResultProcess firstProcess = processesQueue.peek();
            if (firstProcess != null && firstProcess.need2Blocked()) {
                log.warn("Asynchronous deliver block. info={}", firstProcess.toString());
                return false;
            }

            PutResultProcess resultProcess = deliverMessage(msgInner, msgId, offset, offsetPy, sizePy, true);
            processesQueue.add(resultProcess);
            return true;
        }

        private PutResultProcess deliverMessage(MessageExtBrokerInner msgInner, String msgId, long offset,
            long offsetPy, int sizePy, boolean autoResend) {
            CompletableFuture<PutMessageResult> future =
                brokerController.getEscapeBridge().asyncPutMessage(msgInner);
            return new PutResultProcess()
                .setTopic(msgInner.getTopic())
                .setDelayLevel(this.delayLevel)
                .setOffset(offset)
                .setPhysicOffset(offsetPy)
                .setPhysicSize(sizePy)
                .setMsgId(msgId)
                .setAutoResend(autoResend)
                .setFuture(future)
                .thenProcess();
        }
    }

    public class HandlePutResultTask implements Runnable {
        private final int delayLevel;

        public HandlePutResultTask(int delayLevel) {
            this.delayLevel = delayLevel;
        }

        @Override
        public void run() {
            LinkedBlockingQueue<PutResultProcess> pendingQueue =
                ScheduleMessageService.this.deliverPendingTable.get(this.delayLevel);

            PutResultProcess putResultProcess;
            while ((putResultProcess = pendingQueue.peek()) != null) {
                try {
                    switch (putResultProcess.getStatus()) {
                        case SUCCESS:
                            ScheduleMessageService.this.updateOffset(this.delayLevel, putResultProcess.getNextOffset());
                            pendingQueue.remove();
                            break;
                        case RUNNING:
                            break;
                        case EXCEPTION:
                            if (!isStarted()) {
                                log.warn("HandlePutResultTask shutdown, info={}", putResultProcess.toString());
                                return;
                            }
                            log.warn("putResultProcess error, info={}", putResultProcess.toString());
                            putResultProcess.doResend();
                            break;
                        case SKIP:
                            log.warn("putResultProcess skip, info={}", putResultProcess.toString());
                            pendingQueue.remove();
                            break;
                    }
                } catch (Exception e) {
                    log.error("HandlePutResultTask exception. info={}", putResultProcess.toString(), e);
                    putResultProcess.doResend();
                }
            }

            if (isStarted()) {
                ScheduleMessageService.this.handleExecutorService
                    .schedule(new HandlePutResultTask(this.delayLevel), DELAY_FOR_A_SLEEP, TimeUnit.MILLISECONDS);
            }
        }
    }

    public class PutResultProcess {
        /**
         * topic
         */
        private String topic;
        /**
         * 偏移量
         */
        private long offset;
        private long physicOffset;
        private int physicSize;
        /**
         * 延迟等级
         */
        private int delayLevel;
        private String msgId;
        /**
         * 是否自动重发
         */
        private boolean autoResend = false;
        private CompletableFuture<PutMessageResult> future;
        /**
         * 重发计数
         */
        private volatile int resendCount = 0;
        /**
         * 处理状态
         */
        private volatile ProcessStatus status = ProcessStatus.RUNNING;

        public PutResultProcess setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public PutResultProcess setOffset(long offset) {
            this.offset = offset;
            return this;
        }

        public PutResultProcess setPhysicOffset(long physicOffset) {
            this.physicOffset = physicOffset;
            return this;
        }

        public PutResultProcess setPhysicSize(int physicSize) {
            this.physicSize = physicSize;
            return this;
        }

        public PutResultProcess setDelayLevel(int delayLevel) {
            this.delayLevel = delayLevel;
            return this;
        }

        public PutResultProcess setMsgId(String msgId) {
            this.msgId = msgId;
            return this;
        }

        public PutResultProcess setAutoResend(boolean autoResend) {
            this.autoResend = autoResend;
            return this;
        }

        public PutResultProcess setFuture(CompletableFuture<PutMessageResult> future) {
            this.future = future;
            return this;
        }

        public String getTopic() {
            return topic;
        }

        public long getOffset() {
            return offset;
        }

        public long getNextOffset() {
            return offset + 1;
        }

        public long getPhysicOffset() {
            return physicOffset;
        }

        public int getPhysicSize() {
            return physicSize;
        }

        public Integer getDelayLevel() {
            return delayLevel;
        }

        public String getMsgId() {
            return msgId;
        }

        public boolean isAutoResend() {
            return autoResend;
        }

        public CompletableFuture<PutMessageResult> getFuture() {
            return future;
        }

        public int getResendCount() {
            return resendCount;
        }

        public PutResultProcess thenProcess() {
            this.future.thenAccept(result -> {
                this.handleResult(result);
            });

            this.future.exceptionally(e -> {
                log.error("ScheduleMessageService put message exceptionally, info: {}",
                    PutResultProcess.this.toString(), e);

                onException();
                return null;
            });
            return this;
        }

        private void handleResult(PutMessageResult result) {
            if (result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                onSuccess(result);
            } else {
                log.warn("ScheduleMessageService put message failed. info: {}.", result);
                onException();
            }
        }

        public void onSuccess(PutMessageResult result) {
            this.status = ProcessStatus.SUCCESS;
            if (ScheduleMessageService.this.brokerController.getMessageStore().getMessageStoreConfig().isEnableScheduleMessageStats() && !result.isRemotePut()) {
                ScheduleMessageService.this.brokerController.getBrokerStatsManager().incQueueGetNums(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, delayLevel - 1, result.getAppendMessageResult().getMsgNum());
                ScheduleMessageService.this.brokerController.getBrokerStatsManager().incQueueGetSize(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, delayLevel - 1, result.getAppendMessageResult().getWroteBytes());
                ScheduleMessageService.this.brokerController.getBrokerStatsManager().incGroupGetNums(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, result.getAppendMessageResult().getMsgNum());
                ScheduleMessageService.this.brokerController.getBrokerStatsManager().incGroupGetSize(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, result.getAppendMessageResult().getWroteBytes());
                ScheduleMessageService.this.brokerController.getBrokerStatsManager().incTopicPutNums(this.topic, result.getAppendMessageResult().getMsgNum(), 1);
                ScheduleMessageService.this.brokerController.getBrokerStatsManager().incTopicPutSize(this.topic, result.getAppendMessageResult().getWroteBytes());
                ScheduleMessageService.this.brokerController.getBrokerStatsManager().incBrokerPutNums(result.getAppendMessageResult().getMsgNum());
            }
        }

        public void onException() {
            log.warn("ScheduleMessageService onException, info: {}", this.toString());
            if (this.autoResend) {
                this.status = ProcessStatus.EXCEPTION;
            } else {
                this.status = ProcessStatus.SKIP;
            }
        }

        public ProcessStatus getStatus() {
            return this.status;
        }

        public PutMessageResult get() {
            try {
                return this.future.get();
            } catch (InterruptedException | ExecutionException e) {
                return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
            }
        }

        public void doResend() {
            log.info("Resend message, info: {}", this.toString());

            // Gradually increase the resend interval.
            try {
                Thread.sleep(Math.min(this.resendCount++ * 100, 60 * 1000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                MessageExt msgExt = ScheduleMessageService.this.brokerController.getMessageStore().lookMessageByOffset(this.physicOffset, this.physicSize);
                if (msgExt == null) {
                    log.warn("ScheduleMessageService resend not found message. info: {}", this.toString());
                    this.status = need2Skip() ? ProcessStatus.SKIP : ProcessStatus.EXCEPTION;
                    return;
                }

                MessageExtBrokerInner msgInner = ScheduleMessageService.this.messageTimeup(msgExt);
                PutMessageResult result = ScheduleMessageService.this.brokerController.getEscapeBridge().putMessage(msgInner);
                this.handleResult(result);
                if (result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                    log.info("Resend message success, info: {}", this.toString());
                }
            } catch (Exception e) {
                this.status = ProcessStatus.EXCEPTION;
                log.error("Resend message error, info: {}", this.toString(), e);
            }
        }

        public boolean need2Blocked() {
            int maxResendNum2Blocked = ScheduleMessageService.this.brokerController.getMessageStore().getMessageStoreConfig()
                .getScheduleAsyncDeliverMaxResendNum2Blocked();
            return this.resendCount > maxResendNum2Blocked;
        }

        public boolean need2Skip() {
            int maxResendNum2Blocked = ScheduleMessageService.this.brokerController.getMessageStore().getMessageStoreConfig()
                .getScheduleAsyncDeliverMaxResendNum2Blocked();
            return this.resendCount > maxResendNum2Blocked * 2;
        }

        @Override
        public String toString() {
            return "PutResultProcess{" +
                "topic='" + topic + '\'' +
                ", offset=" + offset +
                ", physicOffset=" + physicOffset +
                ", physicSize=" + physicSize +
                ", delayLevel=" + delayLevel +
                ", msgId='" + msgId + '\'' +
                ", autoResend=" + autoResend +
                ", resendCount=" + resendCount +
                ", status=" + status +
                '}';
        }
    }

    public enum ProcessStatus {
        /**
         * 处理中,处理结果还没有返回
         * In process, the processing result has not yet been returned.
         */
        RUNNING,

        /**
         * put 消息成功
         * Put message success.
         */
        SUCCESS,

        /**
         * put 消息 发生异常,若果设置 autoResend 消息将被重新发送
         * Put message exception. When autoResend is true, the message will be resend.
         */
        EXCEPTION,

        /**
         * 跳过 put 消息 ,当消息 不能 上锁 消息会被跳过
         * Skip put message. When the message cannot be looked, the message will be skipped.
         */
        SKIP,
    }

    public ConcurrentMap<Integer, Long> getOffsetTable() {
        return offsetTable;
    }
}
