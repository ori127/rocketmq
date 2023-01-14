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

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.rocketmq.store.logfile.MappedFile;

/**
 * Extend of consume queue, to store something not important,
 * such as message store time, filter bit map and etc.
 * <p/>
 * <li>1. This class is used only by {@link ConsumeQueue}</li>
 * <li>2. And is week reliable.</li>
 * <li>3. Be careful, address returned is always less than 0.</li>
 * <li>4. Pls keep this file small.</li>
 */
public class ConsumeQueueExt {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * 映射文件存储
     */
    private final MappedFileQueue mappedFileQueue;
    /**
     * topic
     */
    private final String topic;
    /**
     * 消费队列
     */
    private final int queueId;
    /**
     * /store/consumequeue_ext 存储路径
     */
    private final String storePath;
    /**
     * 48M 映射文件大小
     */
    private final int mappedFileSize;
    private ByteBuffer tempContainer;
    /**
     * 结束空白大小 4个 字节
     */
    public static final int END_BLANK_DATA_LENGTH = 4;

    /**
     *  MAX_ADDR 为 Integer.MAX_VALUE 超过 Integer.MAX_VALUE
     * Addr can not exceed this value.For compatible.
     */
    public static final long MAX_ADDR = Integer.MIN_VALUE - 1L;
    /**
     * 最大的真实偏移量
     */
    public static final long MAX_REAL_OFFSET = MAX_ADDR - Long.MIN_VALUE;

    /**
     * Constructor.
     *
     * @param topic topic
     * @param queueId id of queue
     * @param storePath root dir of files to store.
     * @param mappedFileSize file size
     * @param bitMapLength bit map length.
     */
    public ConsumeQueueExt(final String topic,
        final int queueId,
        final String storePath,
        final int mappedFileSize,
        final int bitMapLength) {

        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;

        this.topic = topic;
        this.queueId = queueId;
        ///store/consumequeue_ext/{topic}/{queueId}
        String queueDir = this.storePath
            + File.separator + topic
            + File.separator + queueId;

        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

        if (bitMapLength > 0) {
            this.tempContainer = ByteBuffer.allocate(
                bitMapLength / Byte.SIZE
            );
        }
    }

    public long getTotalSize() {
        return this.mappedFileQueue.getTotalFileSize();
    }

    /**
     * 是否超过  Integer.MAX_VALUE 最大值 小于 MAX_ADDR
     * Check whether {@code address} point to extend file.
     * <p>
     * Just test {@code address} is less than 0.
     * </p>
     */
    public static boolean isExtAddr(final long address) {
        //超过  Integer.MAX_VALUE 最大值
        return address <= MAX_ADDR;
    }

    /**
     * Transform {@code address}(decorated by {@link #decorate}) to offset in mapped file.
     * <p>
     * if {@code address} is less than 0, return {@code address} - {@link java.lang.Long#MIN_VALUE};
     * else, just return {@code address}
     * </p>
     */
    public long unDecorate(final long address) {
        //是否超过  Integer.MAX_VALUE 最大值 小于 MAX_ADDR 则 - Long.MIN_VALUE
        if (isExtAddr(address)) {
            return address - Long.MIN_VALUE;
        }
        return address;
    }

    /**
     * Decorate {@code offset} from mapped file, in order to distinguish with tagsCode(saved in cq originally).
     * <p>
     * if {@code offset} is greater than or equal to 0, then return {@code offset} + {@link java.lang.Long#MIN_VALUE};
     * else, just return {@code offset}
     * </p>
     *
     * @return ext address(value is less than 0)
     */
    public long decorate(final long offset) {
        if (!isExtAddr(offset)) {
            return offset + Long.MIN_VALUE;
        }
        return offset;
    }

    /**
     * 根据该偏移量 为 cqExtUnit 生成对应 属性
     * Get data from buffer.
     *
     * @param address less than 0
     */
    public CqExtUnit get(final long address) {
        CqExtUnit cqExtUnit = new CqExtUnit();
        if (get(address, cqExtUnit)) {
            return cqExtUnit;
        }

        return null;
    }

    /**
     * 根据该偏移量 为 cqExtUnit 生成对应 属性
     * Get data from buffer, and set to {@code cqExtUnit}
     *
     * @param address less than 0
     */
    public boolean get(final long address, final CqExtUnit cqExtUnit) {
        if (!isExtAddr(address)) {
            return false;
        }

        final int mappedFileSize = this.mappedFileSize;
        //获取真实的偏移量
        final long realOffset = unDecorate(address);
        //根据该偏移找到对应的映射文件
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(realOffset, realOffset == 0);
        if (mappedFile == null) {
            return false;
        }
        //计算该 偏移量的 pos 定位 获取 偏移对应 ByteBuffer
        int pos = (int) (realOffset % mappedFileSize);

        SelectMappedBufferResult bufferResult = mappedFile.selectMappedBuffer(pos);
        if (bufferResult == null) {
            log.warn("[BUG] Consume queue extend unit({}) is not found!", realOffset);
            return false;
        }
        boolean ret = false;
        try {
            //从 buffer 当前 位置 生成 CqExtUnit的 属性
            ret = cqExtUnit.read(bufferResult.getByteBuffer());
        } finally {
            bufferResult.release();
        }

        return ret;
    }

    /**
     * Save to mapped buffer of file and return address.
     * <p>
     * Be careful, this method is not thread safe.
     * </p>
     *
     * @return success: < 0: fail: >=0
     */
    public long put(final CqExtUnit cqExtUnit) {
        final int retryTimes = 3;
        try {
            //计算 当前 数据 的单元大小
            int size = cqExtUnit.calcUnitSize();
            if (size > CqExtUnit.MAX_EXT_UNIT_SIZE) {
                log.error("Size of cq ext unit is greater than {}, {}", CqExtUnit.MAX_EXT_UNIT_SIZE, cqExtUnit);
                return 1;
            }
            if (this.mappedFileQueue.getMaxOffset() + size > MAX_REAL_OFFSET) {
                log.warn("Capacity of ext is maximum!{}, {}", this.mappedFileQueue.getMaxOffset(), size);
                return 1;
            }
            // unit size maybe change.but, the same most of the time.
            //小于大小 重新 分配 tempContainer
            if (this.tempContainer == null || this.tempContainer.capacity() < size) {
                this.tempContainer = ByteBuffer.allocate(size);
            }

            for (int i = 0; i < retryTimes; i++) {
                //获取最后一个映射文件 如果满了 则重新生成 文件
                MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

                if (mappedFile == null || mappedFile.isFull()) {
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                }

                if (mappedFile == null) {
                    log.error("Create mapped file when save consume queue extend, {}", cqExtUnit);
                    continue;
                }
                //计算可写的大小
                final int wrotePosition = mappedFile.getWrotePosition();
                final int blankSize = this.mappedFileSize - wrotePosition - END_BLANK_DATA_LENGTH;
                //写入打大小超过 文件剩余的大小
                // check whether has enough space.
                if (size > blankSize) {
                    fullFillToEnd(mappedFile, wrotePosition);
                    log.info("No enough space(need:{}, has:{}) of file {}, so fill to end",
                        size, blankSize, mappedFile.getFileName());
                    continue;
                }
                //向 最后一个映射文件 写入 cqExtUnit
                if (mappedFile.appendMessage(cqExtUnit.write(this.tempContainer), 0, size)) {
                    return decorate(wrotePosition + mappedFile.getFileFromOffset());
                }
            }
        } catch (Throwable e) {
            log.error("Save consume queue extend error, " + cqExtUnit, e);
        }

        return 1;
    }

    protected void fullFillToEnd(final MappedFile mappedFile, final int wrotePosition) {
        ByteBuffer mappedFileBuffer = mappedFile.sliceByteBuffer();
        mappedFileBuffer.position(wrotePosition);
        //FIXME:: 为什么个 short 不是应该 是 int 么 ?
        // ending.
        mappedFileBuffer.putShort((short) -1);

        mappedFile.setWrotePosition(this.mappedFileSize);
    }

    /**
     * Load data from file when startup.
     */
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue extend" + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        return result;
    }

    /**
     * Check whether the step size in mapped file queue is correct.
     */
    public void checkSelf() {
        this.mappedFileQueue.checkSelf();
    }

    /**
     * Recover.
     */
    public void recover() {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (mappedFiles == null || mappedFiles.isEmpty()) {
            return;
        }

        // load all files, consume queue will truncate extend files.
        int index = 0;

        MappedFile mappedFile = mappedFiles.get(index);
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        long processOffset = mappedFile.getFileFromOffset();
        long mappedFileOffset = 0;
        CqExtUnit extUnit = new CqExtUnit();
        while (true) {
            extUnit.readBySkip(byteBuffer);
            //若果大小 大于 0 一直读取 读到最后
            // check whether write sth.
            if (extUnit.getSize() > 0) {
                mappedFileOffset += extUnit.getSize();
                continue;
            }
            //取 读到文件最后 获取下一个文件 进行 读取
            index++;
            if (index < mappedFiles.size()) {
                mappedFile = mappedFiles.get(index);
                byteBuffer = mappedFile.sliceByteBuffer();
                processOffset = mappedFile.getFileFromOffset();
                mappedFileOffset = 0;
                log.info("Recover next consume queue extend file, " + mappedFile.getFileName());
                continue;
            }

            log.info("All files of consume queue extend has been recovered over, last mapped file "
                + mappedFile.getFileName());
            break;
        }
        //记录处理的偏移量
        processOffset += mappedFileOffset;
        this.mappedFileQueue.setFlushedWhere(processOffset);
        this.mappedFileQueue.setCommittedWhere(processOffset);
        this.mappedFileQueue.truncateDirtyFiles(processOffset);
    }

    /**
     * Delete files before {@code minAddress}.
     *
     * @param minAddress less than 0
     */
    public void truncateByMinAddress(final long minAddress) {
        if (!isExtAddr(minAddress)) {
            return;
        }

        log.info("Truncate consume queue ext by min {}.", minAddress);

        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();

        List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        final long realOffset = unDecorate(minAddress);
        //遍历映射文件小于 minAddress 进删除
        for (MappedFile file : mappedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;

            if (fileTailOffset < realOffset) {
                log.info("Destroy consume queue ext by min: file={}, fileTailOffset={}, minOffset={}", file.getFileName(),
                    fileTailOffset, realOffset);
                if (file.destroy(1000)) {
                    willRemoveFiles.add(file);
                }
            }
        }
        // 移除失效 映射文件文件
        this.mappedFileQueue.deleteExpiredFile(willRemoveFiles);
    }

    /**
     * Delete files after {@code maxAddress}, and reset wrote/commit/flush position to last file.
     *
     * @param maxAddress less than 0
     */
    public void truncateByMaxAddress(final long maxAddress) {
        if (!isExtAddr(maxAddress)) {
            return;
        }

        log.info("Truncate consume queue ext by max {}.", maxAddress);

        CqExtUnit cqExtUnit = get(maxAddress);
        if (cqExtUnit == null) {
            log.error("[BUG] address {} of consume queue extend not found!", maxAddress);
            return;
        }

        final long realOffset = unDecorate(maxAddress);
        //删除 映射 文件开始偏移量 比这个 大的 映射文件 从映射文件中移除
        this.mappedFileQueue.truncateDirtyFiles(realOffset + cqExtUnit.getSize());
    }

    /**
     * flush buffer to file.
     */
    public boolean flush(final int flushLeastPages) {
        return this.mappedFileQueue.flush(flushLeastPages);
    }

    /**
     * delete files and directory.
     */
    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    /**
     * Max address(value is less than 0).
     * <p/>
     * <p>
     * Be careful: it's an address just when invoking this method.
     * </p>
     */
    public long getMaxAddress() {
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile == null) {
            return decorate(0);
        }
        return decorate(mappedFile.getFileFromOffset() + mappedFile.getWrotePosition());
    }

    /**
     * Minus address saved in file.
     */
    public long getMinAddress() {
        MappedFile firstFile = this.mappedFileQueue.getFirstMappedFile();
        if (firstFile == null) {
            return decorate(0);
        }
        return decorate(firstFile.getFileFromOffset());
    }

    /**
     * Store unit.
     */
    public static class CqExtUnit {
        public static final short MIN_EXT_UNIT_SIZE
            = 2 * 1 // size, 32k max 大小 最大 32 k
            + 8 * 2 // msg time + tagCode 存储时间 + tags 的 hash
            + 2; // bitMapSize 存储 filterBitMap 的大小

        public static final int MAX_EXT_UNIT_SIZE = Short.MAX_VALUE;

        public CqExtUnit() {
        }

        public CqExtUnit(Long tagsCode, long msgStoreTime, byte[] filterBitMap) {
            this.tagsCode = tagsCode == null ? 0 : tagsCode;
            this.msgStoreTime = msgStoreTime;
            this.filterBitMap = filterBitMap;
            this.bitMapSize = (short) (filterBitMap == null ? 0 : filterBitMap.length);
            this.size = (short) (MIN_EXT_UNIT_SIZE + this.bitMapSize);
        }

        /**
         * 2个 字节 大小
         * unit size
         */
        private short size;
        /**
         * tags 的 hash
         * has code of tags
         */
        private long tagsCode;
        /**
         * 8 个 字节 存储时间
         * the time to store into commit log of message
         */
        private long msgStoreTime;
        /**
         * 2个字节 filterBitMap 的 大小
         * size of bit map
         */
        private short bitMapSize;
        /**
         * 过滤的 bit map
         * filter bit map
         */
        private byte[] filterBitMap;

        /**
         * 从 buffer 当前 位置 生成 CqExtUnit的 属性
         * build unit from buffer from current position.
         */
        private boolean read(final ByteBuffer buffer) {
            if (buffer.position() + 2 > buffer.limit()) {
                return false;
            }
            //获取 size 大小 ,tagsCode , msgStoreTime 存储时间 , bitMapSize filterBitMap 的 大小, filterBitMap
            this.size = buffer.getShort();

            if (this.size < 1) {
                return false;
            }

            this.tagsCode = buffer.getLong();
            this.msgStoreTime = buffer.getLong();
            this.bitMapSize = buffer.getShort();

            if (this.bitMapSize < 1) {
                return true;
            }

            if (this.filterBitMap == null || this.filterBitMap.length != this.bitMapSize) {
                this.filterBitMap = new byte[bitMapSize];
            }

            buffer.get(this.filterBitMap);
            return true;
        }

        /**
         * 读取前2个字节即可获得 unit size
         * Only read first 2 byte to get unit size.
         * <p>
         * if size > 0, then skip buffer position with size.
         * </p>
         * <p>
         * if size <= 0, nothing to do.
         * </p>
         */
        private void readBySkip(final ByteBuffer buffer) {
            ByteBuffer temp = buffer.slice();

            short tempSize = temp.getShort();
            this.size = tempSize;

            if (tempSize > 0) {
                buffer.position(buffer.position() + this.size);
            }
        }

        /**
         * Transform unit data to byte array.
         * <p/>
         * <li>1. @{code container} can be null, it will be created if null.</li>
         * <li>2. if capacity of @{code container} is less than unit size, it will be created also.</li>
         * <li>3. Pls be sure that size of unit is not greater than {@link #MAX_EXT_UNIT_SIZE}</li>
         */
        private byte[] write(final ByteBuffer container) {
            //bitMapSize 为 filterBitMap 的大小 大小为 MIN_EXT_UNIT_SIZE + filterBitMap 的大小
            this.bitMapSize = (short) (filterBitMap == null ? 0 : filterBitMap.length);
            this.size = (short) (MIN_EXT_UNIT_SIZE + this.bitMapSize);

            ByteBuffer temp = container;
            //根据大小 分配 ByteBuffer
            if (temp == null || temp.capacity() < this.size) {
                temp = ByteBuffer.allocate(this.size);
            }
            //将 CqExtUnit 写入 ByteBuffer
            temp.flip();
            temp.limit(this.size);

            temp.putShort(this.size);
            temp.putLong(this.tagsCode);
            temp.putLong(this.msgStoreTime);
            temp.putShort(this.bitMapSize);
            if (this.bitMapSize > 0) {
                temp.put(this.filterBitMap);
            }

            return temp.array();
        }

        /**
         * 计算 当前 数据 的单元大小
         * Calculate unit size by current data.
         */
        private int calcUnitSize() {
            int sizeTemp = MIN_EXT_UNIT_SIZE + (filterBitMap == null ? 0 : filterBitMap.length);
            return sizeTemp;
        }

        public long getTagsCode() {
            return tagsCode;
        }

        public void setTagsCode(final long tagsCode) {
            this.tagsCode = tagsCode;
        }

        public long getMsgStoreTime() {
            return msgStoreTime;
        }

        public void setMsgStoreTime(final long msgStoreTime) {
            this.msgStoreTime = msgStoreTime;
        }

        public byte[] getFilterBitMap() {
            if (this.bitMapSize < 1) {
                return null;
            }
            return filterBitMap;
        }

        public void setFilterBitMap(final byte[] filterBitMap) {
            this.filterBitMap = filterBitMap;
            // not safe transform, but size will be calculate by #calcUnitSize
            this.bitMapSize = (short) (filterBitMap == null ? 0 : filterBitMap.length);
        }

        public short getSize() {
            return size;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof CqExtUnit))
                return false;

            CqExtUnit cqExtUnit = (CqExtUnit) o;

            if (bitMapSize != cqExtUnit.bitMapSize)
                return false;
            if (msgStoreTime != cqExtUnit.msgStoreTime)
                return false;
            if (size != cqExtUnit.size)
                return false;
            if (tagsCode != cqExtUnit.tagsCode)
                return false;
            if (!Arrays.equals(filterBitMap, cqExtUnit.filterBitMap))
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) size;
            result = 31 * result + (int) (tagsCode ^ (tagsCode >>> 32));
            result = 31 * result + (int) (msgStoreTime ^ (msgStoreTime >>> 32));
            result = 31 * result + (int) bitMapSize;
            result = 31 * result + (filterBitMap != null ? Arrays.hashCode(filterBitMap) : 0);
            return result;
        }

        @Override
        public String toString() {
            return "CqExtUnit{" +
                "size=" + size +
                ", tagsCode=" + tagsCode +
                ", msgStoreTime=" + msgStoreTime +
                ", bitMapSize=" + bitMapSize +
                ", filterBitMap=" + Arrays.toString(filterBitMap) +
                '}';
        }
    }
}
