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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.FileQueueLifeCycle;
import org.apache.rocketmq.store.queue.QueueOffsetAssigner;
import org.apache.rocketmq.store.queue.ReferredIterator;

public class ConsumeQueue implements ConsumeQueueInterface, FileQueueLifeCycle {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * 存储大小为 20个 字节 8个 字节表示 偏移量  4个字节表示 大小 剩余 8 个字节 表示 tagsCode
     */
    public static final int CQ_STORE_UNIT_SIZE = 20;
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private final MessageStore messageStore;
    /**
     *  队列的映射文件
     */

    private final MappedFileQueue mappedFileQueue;
    /**
     * topic
     */
    private final String topic;
    /**
     * 消息队列 id
     */
    private final int queueId;
    /**
     * 20个字节
     */
    private final ByteBuffer byteBufferIndex;
    /**
     * 存储路径 /store/consumequeue/
     */
    private final String storePath;
    /**
     * 映射文件大小
     */
    private final int mappedFileSize;
    /**
     * 记录最大的物理偏移量
     */
    private long maxPhysicOffset = -1;

    /**
     * 指向有效提交日志记录的消费文件队列的最小偏移量。
     * Minimum offset of the consume file queue that points to valid commit log record.
     */
    private volatile long minLogicOffset = 0;
    private ConsumeQueueExt consumeQueueExt = null;

    public ConsumeQueue(
        final String topic,
        final int queueId,
        final String storePath,
        final int mappedFileSize,
        final MessageStore messageStore) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.messageStore = messageStore;

        this.topic = topic;
        this.queueId = queueId;
        //队列目录 "/store/consumequeue/{topic}/{queueId}"
        String queueDir = this.storePath
            + File.separator + topic
            + File.separator + queueId;

        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);

        if (messageStore.getMessageStoreConfig().isEnableConsumeQueueExt()) {
            this.consumeQueueExt = new ConsumeQueueExt(
                topic,
                queueId,
                StorePathConfigHelper.getStorePathConsumeQueueExt(messageStore.getMessageStoreConfig().getStorePathRootDir()),
                messageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueueExt(),
                messageStore.getMessageStoreConfig().getBitMapLengthConsumeQueueExt()
            );
        }
    }

    @Override
    public boolean load() {
        //加载队列的映射文件
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        if (isExtReadEnable()) {
            result &= this.consumeQueueExt.load();
        }
        return result;
    }

    @Override
    public void recover() {
        //获取该消费队列的映射文件
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            //index 为 倒数 第三个
            int index = mappedFiles.size() - 3;
            if (index < 0) {
                index = 0;
            }
            //映射文件大小
            int mappedFileSizeLogics = this.mappedFileSize;
            //获取 最后第三个映射 文件 创建 副本
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            //最后第三个文件 的开始偏移量
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            long maxExtAddr = 1;
            while (true) {
                //遍历映射文件
                for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    if (offset >= 0 && size > 0) {
                        //下次偏移量
                        mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                        //最大的物理偏移量
                        this.maxPhysicOffset = offset + size;
                        //如果tagsCode 是扩展地址
                        if (isExtAddr(tagsCode)) {
                            maxExtAddr = tagsCode;
                        }
                    } else {
                        log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " "
                            + offset + " " + size + " " + tagsCode);
                        break;
                    }
                }
                //偏移大小 等于 文件大小 说明已经达到 文件结尾
                if (mappedFileOffset == mappedFileSizeLogics) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        //已经全部恢复完毕
                        log.info("recover last consume queue file over, last mapped file "
                            + mappedFile.getFileName());
                        break;
                    } else {
                        //恢复下一个文件
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next consume queue file, " + mappedFile.getFileName());
                    }
                } else {
                    //偏移大小 小于 文件大小 说明已经达到 文件结尾
                    log.info("recover current consume queue over " + mappedFile.getFileName() + " "
                        + (processOffset + mappedFileOffset));
                    break;
                }
            }
            //文件开始偏移量 +  映射文件的偏移量 记录 属性的偏移量 提交的偏移量 清楚该 偏移量之后的文件
            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            if (isExtReadEnable()) {
                this.consumeQueueExt.recover();
                log.info("Truncate consume queue extend file by max {}", maxExtAddr);
                this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
            }
        }
    }

    /**
     * 文件大小
     * @return
     */
    @Override
    public long getTotalSize() {
        //总文件大小 + ext 总文件大小
        long totalSize = this.mappedFileQueue.getTotalFileSize();
        if (isExtReadEnable()) {
            totalSize += this.consumeQueueExt.getTotalSize();
        }
        return totalSize;
    }

    /**
     * 根据时间点 获取队列 逻辑偏移量
     * @param timestamp timestamp
     * @return
     */
    @Override
    public long getOffsetInQueueByTime(final long timestamp) {
        //获取映射文件 修改时间 大于 这个时间 映射文件 没有则 获取最后一个
        MappedFile mappedFile = this.mappedFileQueue.getMappedFileByTime(timestamp);
        if (mappedFile != null) {
            long offset = 0;
            //如果最小偏移量 大于 该文件的偏移量 则 计算 该最小偏移量 到 文件的开头 物理偏移量
            int low = minLogicOffset > mappedFile.getFileFromOffset() ? (int) (minLogicOffset - mappedFile.getFileFromOffset()) : 0;
            int high = 0;
            // midOffset 中间偏移量, targetOffset 目标偏移量 , leftOffset 左边偏移量 , rightOffset 右边偏移量
            int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;
            // leftIndexValue 左边的存储时间 , rightIndexValue 右边的存储时间
            long leftIndexValue = -1L, rightIndexValue = -1L;
            //最小的物理偏移量
            long minPhysicOffset = this.messageStore.getMinPhyOffset();
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0);
            if (null != sbr) {
                ByteBuffer byteBuffer = sbr.getByteBuffer();
                high = byteBuffer.limit() - CQ_STORE_UNIT_SIZE;
                try {
                    while (high >= low) {
                        //获取该中间的文件的物理偏移量 byteBuffer position 指向 midOffset文件偏移量 位置
                        midOffset = (low + high) / (2 * CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;
                        byteBuffer.position(midOffset);
                        //获取 midOffset文件偏移量 的物理偏移量
                        long phyOffset = byteBuffer.getLong();
                        int size = byteBuffer.getInt();
                        //如果该物理偏移量 小于 存储最小偏移量 则 从右边开始查找
                        if (phyOffset < minPhysicOffset) {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            continue;
                        }
                        //根据 中间的物理偏移量获取 消息的存储时间
                        long storeTime =
                            this.messageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                        //二分查找法 查找 对应存储 时间
                        if (storeTime < 0) {
                            return 0;
                        } else if (storeTime == timestamp) {
                            //存储时间 是 查找的时间 则 找到对应偏移量
                            targetOffset = midOffset;
                            break;
                        } else if (storeTime > timestamp) {
                            //存储时间 比 查找的时间 大 那从左边边找
                            high = midOffset - CQ_STORE_UNIT_SIZE;
                            rightOffset = midOffset;
                            rightIndexValue = storeTime;
                        } else {
                            //存储时间 比 查找的时间 小 那从右边找
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            leftIndexValue = storeTime;
                        }
                    }
                    //找到对应时间的偏移量 则 offset 为 对应时间的偏移量
                    if (targetOffset != -1) {

                        offset = targetOffset;
                    } else {
                        //没找到对应的偏移量量 并且 该 时间点 一直 在 左边位置 则以右边最接近的偏移量 作为偏移量
                        if (leftIndexValue == -1) {

                            offset = rightOffset;
                        } else if (rightIndexValue == -1) {
                            //没找到对应的偏移量量 并且 该 时间点 一直 在 右边位置 则以左边最接近的偏移量 作为偏移量
                            offset = leftOffset;
                        } else {
                            //没找到对应的偏移量量 则找 最接接近的偏移量 作为 偏移量
                            offset =
                                Math.abs(timestamp - leftIndexValue) > Math.abs(timestamp
                                    - rightIndexValue) ? rightOffset : leftOffset;
                        }
                    }
                    // (mappedFile.getFileFromOffset() + offset) 该 时间点的 偏移量   (mappedFile.getFileFromOffset() + offset) 该 时间点的 顺序
                    return (mappedFile.getFileFromOffset() + offset) / CQ_STORE_UNIT_SIZE;
                } finally {
                    sbr.release();
                }
            }
        }
        return 0;
    }

    @Override
    public void truncateDirtyLogicFiles(long phyOffset) {
        truncateDirtyLogicFiles(phyOffset, true);
    }

    public void truncateDirtyLogicFiles(long phyOffset, boolean deleteFile) {

        int logicFileSize = this.mappedFileSize;

        this.maxPhysicOffset = phyOffset;
        long maxExtAddr = 1;
        boolean shouldDeleteFile = false;
        while (true) {
            //获取 最后一个映射文件 将 映射文件 写指向 提交指向 刷新指向 设置成 0
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

                mappedFile.setWrotePosition(0);
                mappedFile.setCommittedPosition(0);
                mappedFile.setFlushedPosition(0);

                for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();
                    //第一个 消息物理 偏移量 大于 要找的 phyOffset  则 该文件需要被删除 FIXME ::否则 只修改 映射文件的指向 进行覆盖 写入
                    // 第一个 消息物理  偏移量 小于 要找的 phyOffset 则 增加 CQ_STORE_UNIT_SIZE 记录最大物理偏移量 修改映射
                    if (0 == i) {
                        if (offset >= phyOffset) {
                            shouldDeleteFile = true;
                            break;
                        } else {
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset + size;
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }
                        }
                    } else {

                        if (offset >= 0 && size > 0) {
                            //接下来 消息的偏移量 大于 要找的 phyOffset
                            if (offset >= phyOffset) {
                                return;
                            }
                            //接下来 消息的偏移量 小于 要找的 phyOffset  则 增加 CQ_STORE_UNIT_SIZE 记录最大物理偏移量 FIXME :: 只修改 映射文件的指向 进行覆盖 写入
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset + size;
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }
                            // pos 达到文件 大小 进行 推出
                            if (pos == logicFileSize) {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                }

                if (shouldDeleteFile) {
                    //删除文件
                    if (deleteFile) {
                        this.mappedFileQueue.deleteLastMappedFile();
                    } else {
                        //删除映射关系
                        this.mappedFileQueue.deleteExpiredFile(Collections.singletonList(this.mappedFileQueue.getLastMappedFile()));
                    }
                }

            } else {
                break;
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
        }
    }

    @Override
    public long getLastOffset() {
        long lastOffset = -1;

        int logicFileSize = this.mappedFileSize;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile != null) {
            //定位到最后一个 单位
            int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
            if (position < 0)
                position = 0;

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            byteBuffer.position(position);
            //定位到最后一个 单位 然后 FIXME:: 然后遍历到 文件大小 那不是会读到脏数据么?
            for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                byteBuffer.getLong();

                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                } else {
                    break;
                }
            }
        }

        return lastOffset;
    }

    /**
     * 进行刷新文件
     * @param flushLeastPages  the minimum number of pages to be flushed
     * @return
     */
    @Override
    public boolean flush(final int flushLeastPages) {
        //进行刷新
        boolean result = this.mappedFileQueue.flush(flushLeastPages);
        if (isExtReadEnable()) {
            result = result & this.consumeQueueExt.flush(flushLeastPages);
        }

        return result;
    }

    @Override
    public int deleteExpiredFile(long offset) {
        //删除这个偏移量 之前的文件
        int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(offset, CQ_STORE_UNIT_SIZE);
        this.correctMinOffset(offset);
        return cnt;
    }

    /**
     * Update minLogicOffset such that entries after it would point to valid commit log address.
     *
     * @param minCommitLogOffset Minimum commit log offset
     */
    @Override
    public void correctMinOffset(long minCommitLogOffset) {
        // Check if the consume queue is the state of deprecation.
        if (minLogicOffset >= mappedFileQueue.getMaxOffset()) {
            log.info("ConsumeQueue[Topic={}, queue-id={}] contains no valid entries", topic, queueId);
            return;
        }

        // Check whether the consume queue maps no valid data at all. This check may cost 1 IO operation.
        // The rationale is that consume queue always preserves the last file. In case there are many deprecated topics,
        // This check would save a lot of efforts.
        //获取最后一个映射文件
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile();
        if (null == lastMappedFile) {
            return;
        }

        SelectMappedBufferResult lastRecord = null;
        try {
            int maxReadablePosition = lastMappedFile.getReadPosition();
            //获取最后一个 单位 ByteBuffer
            lastRecord = lastMappedFile.selectMappedBuffer(maxReadablePosition - ConsumeQueue.CQ_STORE_UNIT_SIZE,
                ConsumeQueue.CQ_STORE_UNIT_SIZE);
            if (null != lastRecord) {
                ByteBuffer buffer = lastRecord.getByteBuffer();
                //获取该 最后一个 单位 ByteBuffer 偏移量
                long commitLogOffset = buffer.getLong();
                if (commitLogOffset < minCommitLogOffset) {
                    // Keep the largest known consume offset, even if this consume-queue contains no valid entries at
                    // all. Let minLogicOffset point to a future slot.
                    //FIXME:: 使得 最小 偏移 指向 该映射文件可读 最大的偏移量 ?? minCommitLogOffset 太大导致的
                    this.minLogicOffset = lastMappedFile.getFileFromOffset() + maxReadablePosition;
                    log.info("ConsumeQueue[topic={}, queue-id={}] contains no valid entries. Min-offset is assigned as: {}.",
                        topic, queueId, getMinOffsetInQueue());
                    return;
                }
            }
        } finally {
            if (null != lastRecord) {
                lastRecord.release();
            }
        }
        //获取第一个映射文件
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        long minExtAddr = 1;
        if (mappedFile != null) {
            // Search from previous min logical offset. Typically, a consume queue file segment contains 300,000 entries
            // searching from previous position saves significant amount of comparisons and IOs
            boolean intact = true; // Assume previous value is still valid
            //计算 start 文件 映射 文件的偏移量 start < 0 说明 minLogicOffset 无效了
            long start = this.minLogicOffset - mappedFile.getFileFromOffset();
            if (start < 0) {
                intact = false;
                start = 0;
            }

            if (start > mappedFile.getReadPosition()) {
                log.error("[Bug][InconsistentState] ConsumeQueue file {} should have been deleted",
                    mappedFile.getFileName());
                return;
            }
            //根据该偏移量获取 该 byteBuffer
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer((int) start);
            if (result == null) {
                log.warn("[Bug] Failed to scan consume queue entries from file on correcting min offset: {}",
                    mappedFile.getFileName());
                return;
            }

            try {
                // No valid consume entries
                if (result.getSize() == 0) {
                    log.debug("ConsumeQueue[topic={}, queue-id={}] contains no valid entries", topic, queueId);
                    return;
                }

                ByteBuffer buffer = result.getByteBuffer().slice();
                // Verify whether the previous value is still valid or not before conducting binary search
                long commitLogOffset = buffer.getLong();
                if (intact && commitLogOffset >= minCommitLogOffset) {
                    log.info("Abort correction as previous min-offset points to {}, which is greater than {}",
                        commitLogOffset, minCommitLogOffset);
                    return;
                }

                // Binary search between range [previous_min_logic_offset, first_file_from_offset + file_size)
                // Note the consume-queue deletion procedure ensures the last entry points to somewhere valid.
                int low = 0;
                int high = result.getSize() - ConsumeQueue.CQ_STORE_UNIT_SIZE;
                while (true) {
                    if (high - low <= ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        //没有找到对应的 commitLogOffset
                        break;
                    }
                    int mid = (low + high) / 2 / ConsumeQueue.CQ_STORE_UNIT_SIZE * ConsumeQueue.CQ_STORE_UNIT_SIZE;
                    buffer.position(mid);
                    commitLogOffset = buffer.getLong();
                    //中间的 commitLogOffset 比 minCommitLogOffset 大 说明在 左边
                    if (commitLogOffset > minCommitLogOffset) {
                        high = mid;
                    } else if (commitLogOffset == minCommitLogOffset) {
                        //找到对应 commitLogOffset
                        low = mid;
                        high = mid;
                        break;
                    } else {
                        //中间的 commitLogOffset 比 minCommitLogOffset 小 说明在 左边
                        low = mid;
                    }
                }

                // Examine the last one or two entries
                for (int i = low; i <= high; i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                    buffer.position(i);
                    //获取该 位置的  commitLogOffset 和 tagsCode
                    long offsetPy = buffer.getLong();
                    buffer.position(i + 12);
                    long tagsCode = buffer.getLong();
                    //如果 该 位置的  commitLogOffset 则记录 minLogicOffset
                    if (offsetPy >= minCommitLogOffset) {
                        this.minLogicOffset = mappedFile.getFileFromOffset() + start + i;
                        log.info("Compute logical min offset: {}, topic: {}, queueId: {}",
                            this.getMinOffsetInQueue(), this.topic, this.queueId);
                        // This maybe not take effect, when not every consume queue has an extended file.
                        if (isExtAddr(tagsCode)) {
                            minExtAddr = tagsCode;
                        }
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("Exception thrown when correctMinOffset", e);
            } finally {
                result.release();
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMinAddress(minExtAddr);
        }
    }

    /**
     * 获取队列当中的最小偏移
     * @return
     */
    @Override
    public long getMinOffsetInQueue() {
        return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
    }

    @Override
    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        final int maxRetries = 30;
        //获取运行标记是否可写
        boolean canWrite = this.messageStore.getRunningFlags().isCQWriteable();
        for (int i = 0; i < maxRetries && canWrite; i++) {
            long tagsCode = request.getTagsCode();
            //为 cqExtUnit 生成 filterBitMap MsgStoreTime TagsCode 属性 写入 cqExtUnit 设置 tagsCode 为 consumeQueueExt 映射文件的 偏移量
            if (isExtWriteEnable()) {
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                cqExtUnit.setFilterBitMap(request.getBitMap());
                cqExtUnit.setMsgStoreTime(request.getStoreTimestamp());
                cqExtUnit.setTagsCode(request.getTagsCode());
                //存储cqExtUnit
                long extAddr = this.consumeQueueExt.put(cqExtUnit);
                if (isExtAddr(extAddr)) {
                    tagsCode = extAddr;
                } else {
                    log.warn("Save consume queue extend fail, So just save tagsCode! {}, topic:{}, queueId:{}, offset:{}", cqExtUnit,
                        topic, queueId, request.getCommitLogOffset());
                }
            }
            //设置消息信息
            boolean result = this.putMessagePositionInfo(request.getCommitLogOffset(),
                request.getMsgSize(), tagsCode, request.getConsumeQueueOffset());
            if (result) {
                //设置物理消息存储点 逻辑消息存储时间
                if (this.messageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE ||
                    this.messageStore.getMessageStoreConfig().isEnableDLegerCommitLog()) {
                    this.messageStore.getStoreCheckpoint().setPhysicMsgTimestamp(request.getStoreTimestamp());
                }
                this.messageStore.getStoreCheckpoint().setLogicsMsgTimestamp(request.getStoreTimestamp());
                //检查是否 MultiDispatchQueue
                if (checkMultiDispatchQueue(request)) {
                    multiDispatchLmqQueue(request, maxRetries);
                }
                return;
            } else {
                // XXX: warn and notify me
                log.warn("[BUG]put commit log position info to " + topic + ":" + queueId + " " + request.getCommitLogOffset()
                    + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }

        // XXX: warn and notify me
        log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
        this.messageStore.getRunningFlags().makeLogicsQueueError();
    }

    private boolean checkMultiDispatchQueue(DispatchRequest dispatchRequest) {
        if (!this.messageStore.getMessageStoreConfig().isEnableMultiDispatch()) {
            return false;
        }
        Map<String, String> prop = dispatchRequest.getPropertiesMap();
        if (prop == null || prop.isEmpty()) {
            return false;
        }
        //判断INNER_MULTI_DISPATCH 和 INNER_MULTI_QUEUE_OFFSET 是否存在
        String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        if (StringUtils.isBlank(multiDispatchQueue) || StringUtils.isBlank(multiQueueOffset)) {
            return false;
        }
        return true;
    }

    private void multiDispatchLmqQueue(DispatchRequest request, int maxRetries) {
        Map<String, String> prop = request.getPropertiesMap();
        String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        //获取属性 PROPERTY_INNER_MULTI_DISPATCH 的 队列名称 为topic名称
        //获取属性 MULTI_DISPATCH_QUEUE_SPLITTER 的 队列 对应 偏移量
        String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        String[] queueOffsets = multiQueueOffset.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        if (queues.length != queueOffsets.length) {
            log.error("[bug] queues.length!=queueOffsets.length ", request.getTopic());
            return;
        }
        //遍历消息队列
        for (int i = 0; i < queues.length; i++) {
            String queueName = queues[i];
            long queueOffset = Long.parseLong(queueOffsets[i]);
            int queueId = request.getQueueId();
            if (this.messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(queueName)) {
                queueId = 0;
            }
            doDispatchLmqQueue(request, maxRetries, queueName, queueOffset, queueId);

        }
        return;
    }

    private void doDispatchLmqQueue(DispatchRequest request, int maxRetries, String queueName, long queueOffset,
        int queueId) {
        //获取 queueName 消息队列 的 ConsumeQueueInterface 映射
        ConsumeQueueInterface cq = this.messageStore.findConsumeQueue(queueName, queueId);
        //获取运行标记是否可写
        boolean canWrite = this.messageStore.getRunningFlags().isCQWriteable();
        //设置消息位置信息
        for (int i = 0; i < maxRetries && canWrite; i++) {
            boolean result = ((ConsumeQueue) cq).putMessagePositionInfo(request.getCommitLogOffset(), request.getMsgSize(),
                request.getTagsCode(),
                queueOffset);
            if (result) {
                break;
            } else {
                log.warn("[BUG]put commit log position info to " + queueName + ":" + queueId + " " + request.getCommitLogOffset()
                    + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }
    }

    @Override
    public void assignQueueOffset(QueueOffsetAssigner queueOffsetAssigner, MessageExtBrokerInner msg,
        short messageNum) {
        //生成 topicQueueKey "topic-queueId"
        String topicQueueKey = getTopic() + "-" + getQueueId();
        //初始化 简单消费队列  topic-queueId 偏移量 设置该消息 队列偏移量
        long queueOffset = queueOffsetAssigner.assignQueueOffset(topicQueueKey, messageNum);
        msg.setQueueOffset(queueOffset);
        // For LMQ
        //为轻量化消息
        if (!messageStore.getMessageStoreConfig().isEnableMultiDispatch()) {
            return;
        }
        //获取消息属性 INNER_MULTI_DISPATCH 根据 "," 进行 分割
        String multiDispatchQueue = msg.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        if (StringUtils.isBlank(multiDispatchQueue)) {
            return;
        }
        String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        Long[] queueOffsets = new Long[queues.length];
        for (int i = 0; i < queues.length; i++) {
            String key = queueKey(queues[i], msg);
            //如果轻量级消息队列 初始化 轻量级 FIXME:: 这边的 key 为什么 是 queueName-queueId ?
            if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(key)) {
                queueOffsets[i] = queueOffsetAssigner.assignLmqOffset(key, (short) 1);
            }
        }
        //为轻量化消息 设置 INNER_MULTI_QUEUE_OFFSET 属性 queueOffsets
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET,
            StringUtils.join(queueOffsets, MixAll.MULTI_DISPATCH_QUEUE_SPLITTER));
        // 设置 propertiesString 则 先将  WAIT 属性移除 生成 propertiesString 然后 则重新 放置 WAIT
        removeWaitStorePropertyString(msg);
    }

    /**
     * 生成 queueKey "queueName-queueId"
     * @param queueName
     * @param msgInner
     * @return
     */
    public String queueKey(String queueName, MessageExtBrokerInner msgInner) {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(queueName);
        keyBuilder.append('-');
        int queueId = msgInner.getQueueId();
        if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(queueName)) {
            queueId = 0;
        }
        keyBuilder.append(queueId);
        return keyBuilder.toString();
    }

    /**
     * 设置 propertiesString 则 先将  WAIT 属性移除 生成 propertiesString 然后 则重新 放置 WAIT
     * @param msgInner
     */
    private void removeWaitStorePropertyString(MessageExtBrokerInner msgInner) {
        //设置 propertiesString 则 先将  WAIT 属性移除 生成 propertiesString 然后 则重新 放置 WAIT
        if (msgInner.getProperties().containsKey(MessageConst.PROPERTY_WAIT_STORE_MSG_OK)) {
            // There is no need to store "WAIT=true", remove it from propertiesString to save 9 bytes for each message.
            // It works for most case. In some cases msgInner.setPropertiesString invoked later and replace it.
            String waitStoreMsgOKValue = msgInner.getProperties().remove(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
            // Reput to properties, since msgInner.isWaitStoreMsgOK() will be invoked later
            msgInner.getProperties().put(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, waitStoreMsgOKValue);
        } else {
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        }
    }

    private boolean putMessagePositionInfo(final long offset, final int size, final long tagsCode,
        final long cqOffset) {

        if (offset + size <= this.maxPhysicOffset) {
            log.warn("Maybe try to build consume queue repeatedly maxPhysicOffset={} phyOffset={}", maxPhysicOffset, offset);
            return true;
        }
        //重置 position mark limit = position;
        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
        //设置 偏移量 大小  tagsCode
        this.byteBufferIndex.putLong(offset);
        this.byteBufferIndex.putInt(size);
        this.byteBufferIndex.putLong(tagsCode);
        //计算该 逻辑偏移量 根据该逻辑 偏移量 获取 对应 的映射文件
        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;
        //获取最后一个文件 ,没有映射文件 存在 则  createOffset 创建文件进行映射
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);
        if (mappedFile != null) {
            //当前文件是否是 队列里面 第一个映射文件 且 队列偏移量 不是 0 , 该映射文件  WrotePosition 是 0
            //记录 minLogicOffset ,flushedWhere ,committedWhere 为 expectLogicOffset 然后 对 expectLogicOffset 之前 单元 进行 填充
            if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) {
                this.minLogicOffset = expectLogicOffset;
                this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
                this.mappedFileQueue.setCommittedWhere(expectLogicOffset);
                this.fillPreBlank(mappedFile, expectLogicOffset);
                log.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " "
                    + mappedFile.getWrotePosition());
            }

            if (cqOffset != 0) {
                //获取 当前文件 的逻辑 偏移量
                long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();

                if (expectLogicOffset < currentLogicOffset) {
                    log.warn("Build  consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
                    return true;
                }

                if (expectLogicOffset != currentLogicOffset) {
                    LOG_ERROR.warn(
                        "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset,
                        currentLogicOffset,
                        this.topic,
                        this.queueId,
                        expectLogicOffset - currentLogicOffset
                    );
                }
            }
            //在 expectLogicOffset 写入 byteBufferIndex
            this.maxPhysicOffset = offset + size;
            return mappedFile.appendMessage(this.byteBufferIndex.array());
        }
        return false;
    }

    /**
     * 对映射文件 进行 填充 直至 到 untilWhere 偏移量位置
     * @param mappedFile
     * @param untilWhere
     */
    private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {
        //分配一个 CQ_STORE_UNIT_SIZE 的 byteBuffer 然后对映射文件 进行 填充 直至 到 untilWhere 偏移量位置
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());
        for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {
            mappedFile.appendMessage(byteBuffer.array());
        }
    }
    /**
     * startIndex * CQ_STORE_UNIT_SIZE 计算该 偏移量 找到 该偏移对应的 映射文件
     *  offset % mappedFileSize 该 偏移量 对应的 文件 pos 获取对应的 byteBuffer
     */
    public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
        int mappedFileSize = this.mappedFileSize;
        //startIndex * CQ_STORE_UNIT_SIZE 计算该 偏移量 找到 该偏移对应的 映射文件
        // offset % mappedFileSize 该 偏移量 对应的 文件 pos 获取对应的 byteBuffer
        long offset = startIndex * CQ_STORE_UNIT_SIZE;
        if (offset >= this.getMinLogicOffset()) {
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
            if (mappedFile != null) {
                return mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
            }
        }
        return null;
    }

    /**
     * 根据该 startIndex 获取 对应 的 ByteBuffer
     * @param startOffset start index
     * @return
     */
    @Override
    public ReferredIterator<CqUnit> iterateFrom(long startOffset) {
        //根据该 startIndex 获取 对应 的 ByteBuffer
        SelectMappedBufferResult sbr = getIndexBuffer(startOffset);
        if (sbr == null) {
            return null;
        }
        return new ConsumeQueueIterator(sbr);
    }

    /**
     * 根据 该 index 获取 CqUnit
     * @param offset index
     * @return
     */
    @Override
    public CqUnit get(long offset) {
        ReferredIterator<CqUnit> it = iterateFrom(offset);
        if (it == null) {
            return null;
        }
        return it.nextAndRelease();
    }

    @Override
    public CqUnit getEarliestUnit() {
        /**
         * here maybe should not return null
         */
        // minLogicOffset / CQ_STORE_UNIT_SIZE 计算为 index 然后根据 该 index 获取 CqUnit
        ReferredIterator<CqUnit> it = iterateFrom(minLogicOffset / CQ_STORE_UNIT_SIZE);
        if (it == null) {
            return null;
        }
        return it.nextAndRelease();
    }

    @Override
    public CqUnit getLatestUnit() {
        // MaxOffset / CQ_STORE_UNIT_SIZE - 1 计算为 index 然后根据 该 index 获取 CqUnit
        ReferredIterator<CqUnit> it = iterateFrom((mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE) - 1);
        if (it == null) {
            return null;
        }
        return it.nextAndRelease();
    }

    @Override
    public boolean isFirstFileAvailable() {
        return false;
    }

    @Override
    public boolean isFirstFileExist() {
        return false;
    }

    private class ConsumeQueueIterator implements ReferredIterator<CqUnit> {
        private SelectMappedBufferResult sbr;
        /**
         * 初始化时 ConsumeQueueIterator ByteBuffer.position
         */
        private int relativePos = 0;

        public ConsumeQueueIterator(SelectMappedBufferResult sbr) {
            this.sbr =  sbr;
            if (sbr != null && sbr.getByteBuffer() != null) {
                relativePos = sbr.getByteBuffer().position();
            }
        }

        /**
         * 是否还有剩余
         * @return
         */
        @Override
        public boolean hasNext() {
            if (sbr == null || sbr.getByteBuffer() == null) {
                return false;
            }

            return sbr.getByteBuffer().hasRemaining();
        }

        @Override
        public CqUnit next() {
            if (!hasNext()) {
                return null;
            }
            //计算 队列的偏移量 FIXME:: 为什么要 - relativePos
            long queueOffset = (sbr.getStartOffset() + sbr.getByteBuffer().position() -  relativePos) / CQ_STORE_UNIT_SIZE;
            CqUnit cqUnit = new CqUnit(queueOffset,
                    sbr.getByteBuffer().getLong(),
                    sbr.getByteBuffer().getInt(),
                    sbr.getByteBuffer().getLong());

            if (isExtAddr(cqUnit.getTagsCode())) {
                //根据该偏移量 为 cqExtUnit 生成对应 属性
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                boolean extRet = getExt(cqUnit.getTagsCode(), cqExtUnit);
                //为 cqUnit 设置真实 tagsCode 设置 cqExtUnit
                if (extRet) {
                    cqUnit.setTagsCode(cqExtUnit.getTagsCode());
                    cqUnit.setCqExtUnit(cqExtUnit);
                } else {
                    // can't find ext content.Client will filter messages by tag also.
                    log.error("[BUG] can't find consume queue extend file content! addr={}, offsetPy={}, sizePy={}, topic={}",
                            cqUnit.getTagsCode(), cqUnit.getPos(), cqUnit.getPos(), getTopic());
                }
            }
            return cqUnit;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove");
        }

        @Override
        public void release() {
            if (sbr != null) {
                sbr.release();
                sbr = null;
            }
        }

        @Override
        public CqUnit nextAndRelease() {
            try {
                return next();
            } finally {
                release();
            }
        }
    }

    /**
     * 根据该偏移量 为 cqExtUnit 生成对应 属性
     * @param offset
     * @return
     */
    public ConsumeQueueExt.CqExtUnit getExt(final long offset) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset);
        }
        return null;
    }

    /**
     * 根据该偏移量 为 cqExtUnit 生成对应 属性
     * @param offset
     * @param cqExtUnit
     * @return
     */
    public boolean getExt(final long offset, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset, cqExtUnit);
        }
        return false;
    }

    @Override
    public long getMinLogicOffset() {
        return minLogicOffset;
    }

    public void setMinLogicOffset(long minLogicOffset) {
        this.minLogicOffset = minLogicOffset;
    }

    @Override
    public long rollNextFile(final long nextBeginOffset) {
        int mappedFileSize = this.mappedFileSize;
        //单个映射文件 总共 单元数量
        int totalUnitsInFile = mappedFileSize / CQ_STORE_UNIT_SIZE;
        //nextBeginOffset  - nextBeginOffset % totalUnitsInFile 该偏移量 当前 文件 的映射 偏移量
        // + totalUnitsInFile  该偏移量 nextBeginOffset 下一个文件的 偏移量
        return nextBeginOffset + totalUnitsInFile - nextBeginOffset % totalUnitsInFile;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public int getQueueId() {
        return queueId;
    }

    @Override
    public CQType getCQType() {
        return CQType.SimpleCQ;
    }

    @Override
    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }

    /**
     * 关闭文件进行删除 删除 父类 目录
     */
    @Override
    public void destroy() {
        this.maxPhysicOffset = -1;
        this.minLogicOffset = 0;
        this.mappedFileQueue.destroy();
        if (isExtReadEnable()) {
            this.consumeQueueExt.destroy();
        }
    }

    /**
     * 该消息队列 中 总共的数量
     * @return
     */
    @Override
    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }

    /**
     * 在队列中最大 的偏移量
     * @return
     */
    @Override
    public long getMaxOffsetInQueue() {
        return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
    }

    @Override
    public void checkSelf() {
        mappedFileQueue.checkSelf();
        if (isExtReadEnable()) {
            this.consumeQueueExt.checkSelf();
        }
    }

    protected boolean isExtReadEnable() {
        return this.consumeQueueExt != null;
    }

    protected boolean isExtWriteEnable() {
        return this.consumeQueueExt != null
            && this.messageStore.getMessageStoreConfig().isEnableConsumeQueueExt();
    }

    /**
     * 检查是否是扩展文件
     * Check {@code tagsCode} is address of extend file or tags code.
     */
    public boolean isExtAddr(long tagsCode) {
        return ConsumeQueueExt.isExtAddr(tagsCode);
    }

    @Override
    public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {
        mappedFileQueue.swapMap(reserveNum, forceSwapIntervalMs, normalSwapIntervalMs);
    }

    @Override
    public void cleanSwappedMap(long forceCleanSwapIntervalMs) {
        mappedFileQueue.cleanSwappedMap(forceCleanSwapIntervalMs);
    }
}
