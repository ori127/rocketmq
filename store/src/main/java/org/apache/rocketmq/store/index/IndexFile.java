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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.List;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;
/**
 * indexFile: indexHeader 记录indexFile信息
 *            hashSlot 记录 index 的位置
 *            index   具体的存储数据
 */
public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     *  hashSlot大小 4个字节
     */
    private static int hashSlotSize = 4;
    /**
     * index大小 20个字节
     */
    private static int indexSize = 20;
    /**
     * 无效 index 标志
     */
    private static int invalidIndex = 0;
    /**
     * hashSlot数量
     */
    private final int hashSlotNum;
    /**
     * index 数量
     */
    private final int indexNum;
    /**
     * 文件总共大小 header 40个字节 + hashSlot数量 * hashSlot大小 4个字节 + index数量 * index大小 20个字节
     */
    private final int fileTotalSize;
    /**
     * indexFile 映射文件
     */
    private final MappedFile mappedFile;
    /**
     * 文件映射文件Buffer
     */
    private final MappedByteBuffer mappedByteBuffer;
    /**
     * indexFile 的 indexHeader 信息 40 个字节
     */
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        //计算文件总共大小 根据大小创建映射文件 
        this.fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new DefaultMappedFile(fileName, fileTotalSize);
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;
        //创建该映射文件副本 提供 indexHeader 操作该映射文件       
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);
        //设置 该映射文件的 结束偏移量
        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }
        //设置 该映射文件的 结束时间
        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public int getFileSize() {
        return this.fileTotalSize;
    }

    public void load() {
        this.indexHeader.load();
    }

    /**
     * 进刷刷新 清理内存
     */
    public void shutdown() {
        this.flush();
        UtilAll.cleanBuffer(this.mappedByteBuffer);
    }

    public void flush() {
        //更新文件 indexHeader 进行刷新
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    /**
     * index 文件是否 满
     * @return
     */
    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    /**
     * 关闭文件进行删除
     * @param intervalForcibly
     * @return
     */
    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            //计算 key的 hash % % this.hashSlotNum 获取该 key 对应的 slot 的位置  计算 该 key 的文件的 位置
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            try {
                //获取该位置slot 的值 如果小于 invalidIndex 或者 超过 indexCount 说明该位置是个 无效index
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }
                //计算存储时间 的时间差 秒
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }
                //计算该key 对应 index 的位置
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;
                //设置 key 的 hash 值 , 物理偏移量, 存储时间差 , slotValue 值
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                //slotValue = 0 ,表示这个 slot 从来没有被使用过 , 如果 slotValue != 0 则表示 这个 slot 已经 被使用  absIndexPos + 4 + 8 + 4 记录 上一个 index
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);
                //在 slot 的 位置设置 index 位置
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());
                //假如 index的数量 小于 等于 1 需要重新设置  header 物理偏移量  和 记录存储时间
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }
                //如果 invalidIndex == slotValue 说明是 有效的 slot invalidIndex != slotValue 则进占用 不用hashSot增加计数
                if (invalidIndex == slotValue) {
                    this.indexHeader.incHashSlotCount();
                }
                //增加 index 计数 记录计 物理偏移量 存储时间
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    /**
     * 计算key的hash位置
     * @param key
     * @return
     */
    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0) {
            keyHashPositive = 0;
        }
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    /**
     * 计算时间是否在这个index 文件内
     * @param begin
     * @param end
     * @return
     */
    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp();
        result = result || end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp();
        return result;
    }

    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
                                final long begin, final long end) {
        if (this.mappedFile.hold()) {
            //计算 key的 hash % % this.hashSlotNum 获取该 key 对应的 slot 的位置  计算 该 key 的文件的 位置
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            try {
                //获取该key 对应的 index 位置
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    //从 nextIndexToRead 开始查找
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }
                        //计算 该 key 对应的 index 的 偏移量
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;
                        //key 对应的 index 的 偏移量 key 的 值 该物理偏移, 存储时间差, absIndexPos + 4 + 8 + 4 同样使用 该 slot 的 index
                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;
                        //是否符合 end >= timeRead >= begin 时间段
                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = timeRead >= begin && timeRead <= end;
                        //如果 key hash 值符合 并且 时间也符合 则添加到  phyOffsetRead
                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }
                        //如果 prevIndexRead 是 无效 的 则停止 查找
                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }
                        //查找 上一个 index
                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                this.mappedFile.release();
            }
        }
    }
}
