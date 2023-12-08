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

package org.apache.rocketmq.store.ha.autoswitch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import org.apache.rocketmq.common.EpochEntry;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.CheckpointFile;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * Cache for epochFile. Mapping (Epoch -> StartOffset)
 */
public class EpochFileCache {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * 读写锁
     */
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    /**
     * 读锁
     */
    private final Lock readLock = this.readWriteLock.readLock();
    /**
     * 写锁
     */
    private final Lock writeLock = this.readWriteLock.writeLock();
    /**
     * Key 为 代数 ,value 为具体的 EpochEntry
     */
    private final TreeMap<Integer, EpochEntry> epochMap;
    /**
     * 检查点文件路径
     */
    private CheckpointFile<EpochEntry> checkpoint;

    public EpochFileCache() {
        this.epochMap = new TreeMap<>();
    }

    /**
     * 根据文件路径恢复 EpochEntry
     * @param path
     */
    public EpochFileCache(final String path) {
        this.epochMap = new TreeMap<>();
        this.checkpoint = new CheckpointFile<>(path, new EpochEntrySerializer());
    }

    /**
     * 从检查点文件读取提条目 初始化条目
     * @return
     */
    public boolean initCacheFromFile() {
        //上锁 从检查点文件读取提条目 初始化条目
        this.writeLock.lock();
        try {
            //从检查点文件读取提条目 初始化条目
            final List<EpochEntry> entries = this.checkpoint.read();
            initEntries(entries);
            return true;
        } catch (final IOException e) {
            log.error("Error happen when init epoch entries from epochFile", e);
            return false;
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 初始条目 写入磁盘
     * @param entries
     */
    public void initCacheFromEntries(final List<EpochEntry> entries) {
        this.writeLock.lock();
        try {
            initEntries(entries);
            flush();
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 初始化 epochMap key 为 代数 并设置 链上的 结束偏移量
     * @param entries
     */
    private void initEntries(final List<EpochEntry> entries) {
        this.epochMap.clear();
        EpochEntry preEntry = null;
        for (final EpochEntry entry : entries) {
            this.epochMap.put(entry.getEpoch(), entry);
            if (preEntry != null) {
                preEntry.setEndOffset(entry.getStartOffset());
            }
            preEntry = entry;
        }
    }

    /**
     * 获取 epochMap 的数量
     * @return
     */

    public int getEntrySize() {
        this.readLock.lock();
        try {
            return this.epochMap.size();
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * 添加条目
     * @param entry
     * @return
     */
    public boolean appendEntry(final EpochEntry entry) {
        this.writeLock.lock();
        try {
            if (!this.epochMap.isEmpty()) {
                //获取最后一个 EpochEntry 校验添加的 EpochEntry 的偏移量 和 代数 是否 比最后 一个 EpochEntry 的 开始的偏移量 和 代数 大
                final EpochEntry lastEntry = this.epochMap.lastEntry().getValue();
                if (lastEntry.getEpoch() >= entry.getEpoch() || lastEntry.getStartOffset() >= entry.getStartOffset()) {
                    log.error("The appending entry's lastEpoch or endOffset {} is not bigger than lastEntry {}, append failed", entry, lastEntry);
                    return false;
                }
                lastEntry.setEndOffset(entry.getStartOffset());
            }
            this.epochMap.put(entry.getEpoch(), new EpochEntry(entry));
            flush();
            return true;
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 设置最后一个条目 结束偏移量
     * Set endOffset for lastEpochEntry.
     */
    public void setLastEpochEntryEndOffset(final long endOffset) {
        this.writeLock.lock();
        try {
            if (!this.epochMap.isEmpty()) {
                final EpochEntry lastEntry = this.epochMap.lastEntry().getValue();
                if (lastEntry.getStartOffset() <= endOffset) {
                    lastEntry.setEndOffset(endOffset);
                }
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 获取第一个条目
     * @return
     */
    public EpochEntry firstEntry() {
        this.readLock.lock();
        try {
            if (this.epochMap.isEmpty()) {
                return null;
            }
            return new EpochEntry(this.epochMap.firstEntry().getValue());
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * 获取最后一个条目
     * @return
     */
    public EpochEntry lastEntry() {
        this.readLock.lock();
        try {
            if (this.epochMap.isEmpty()) {
                return null;
            }
            return new EpochEntry(this.epochMap.lastEntry().getValue());
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * 最后一个代数
     * @return
     */
    public int lastEpoch() {
        final EpochEntry entry = lastEntry();
        if (entry != null) {
            return entry.getEpoch();
        }
        return -1;
    }

    /**
     * 根据代数 获取 条目
     * @param epoch
     * @return
     */
    public EpochEntry getEntry(final int epoch) {
        this.readLock.lock();
        try {
            if (this.epochMap.containsKey(epoch)) {
                final EpochEntry entry = this.epochMap.get(epoch);
                return new EpochEntry(entry);
            }
            return null;
        } finally {
            this.readLock.unlock();
        }
    }

    public EpochEntry findEpochEntryByOffset(final long offset) {
        this.readLock.lock();
        try {
            if (!this.epochMap.isEmpty()) {
                //遍历 epochMap 查找 offset 所在的 EpochEntry
                for (Map.Entry<Integer, EpochEntry> entry : this.epochMap.entrySet()) {
                    if (entry.getValue().getStartOffset() <= offset && entry.getValue().getEndOffset() > offset) {
                        return new EpochEntry(entry.getValue());
                    }
                }
            }
            return null;
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * 根据 epoch 获取下一个 EpochEntry
     * @param epoch
     * @return
     */
    public EpochEntry nextEntry(final int epoch) {
        this.readLock.lock();
        try {
            final Map.Entry<Integer, EpochEntry> entry = this.epochMap.ceilingEntry(epoch + 1);
            if (entry != null) {
                return new EpochEntry(entry.getValue());
            }
            return null;
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * 获取所有的EpochEntry
     * @return
     */
    public List<EpochEntry> getAllEntries() {
        this.readLock.lock();
        try {
            final ArrayList<EpochEntry> result = new ArrayList<>(this.epochMap.size());
            this.epochMap.forEach((key, value) -> result.add(new EpochEntry(value)));
            return result;
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * Find the consistentPoint between compareCache and local.
     *
     * @return the consistent offset
     */
    public long findConsistentPoint(final EpochFileCache compareCache) {
        this.readLock.lock();
        try {
            long consistentOffset = -1;
            final Map<Integer, EpochEntry> descendingMap = new TreeMap<>(this.epochMap).descendingMap();
            final Iterator<Map.Entry<Integer, EpochEntry>> iter = descendingMap.entrySet().iterator();
            while (iter.hasNext()) {
                final Map.Entry<Integer, EpochEntry> curLocalEntry = iter.next();
                final EpochEntry compareEntry = compareCache.getEntry(curLocalEntry.getKey());
                if (compareEntry != null && compareEntry.getStartOffset() == curLocalEntry.getValue().getStartOffset()) {
                    consistentOffset = Math.min(curLocalEntry.getValue().getEndOffset(), compareEntry.getEndOffset());
                    break;
                }
            }
            return consistentOffset;
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * 删除 代数 大于 truncateEpoch 的 条目
     * Remove epochEntries with epoch >= truncateEpoch.
     */
    public void truncateSuffixByEpoch(final int truncateEpoch) {
        Predicate<EpochEntry> predict = entry -> entry.getEpoch() >= truncateEpoch;
        doTruncateSuffix(predict);
    }

    /**
     * 删除 开始偏移 大于truncateOffset 的条目
     * Remove epochEntries with startOffset >= truncateOffset.
     */
    public void truncateSuffixByOffset(final long truncateOffset) {
        Predicate<EpochEntry> predict = entry -> entry.getStartOffset() >= truncateOffset;
        doTruncateSuffix(predict);
    }
    /**
     *  从 epochMap 移除 符合条件的条目 设置最后一个条目的偏移量 为最大 然后刷新磁盘
     */
    private void doTruncateSuffix(Predicate<EpochEntry> predict) {
        this.writeLock.lock();
        try {
            this.epochMap.entrySet().removeIf(entry -> predict.test(entry.getValue()));
            final EpochEntry entry = lastEntry();
            if (entry != null) {
                entry.setEndOffset(Long.MAX_VALUE);
            }
            flush();
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 从 epochMap 移除 结束偏移量  小于  truncateOffset 的条目 然后刷新磁盘
     * Remove epochEntries with endOffset <= truncateOffset.
     */
    public void truncatePrefixByOffset(final long truncateOffset) {
        Predicate<EpochEntry> predict = entry -> entry.getEndOffset() <= truncateOffset;
        this.writeLock.lock();
        try {
            this.epochMap.entrySet().removeIf(entry -> predict.test(entry.getValue()));
            flush();
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * 将条目写入磁盘
     */
    private void flush() {
        this.writeLock.lock();
        try {
            if (this.checkpoint != null) {
                final ArrayList<EpochEntry> entries = new ArrayList<>(this.epochMap.values());
                this.checkpoint.write(entries);
            }
        } catch (final IOException e) {
            log.error("Error happen when flush epochEntries to epochCheckpointFile", e);
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * EpochEntry 的序列化 方式 采用 %d(袋数)-%d(开始偏移量)
     */
    static class EpochEntrySerializer implements CheckpointFile.CheckpointSerializer<EpochEntry> {

        @Override
        public String toLine(EpochEntry entry) {
            if (entry != null) {
                return String.format("%d-%d", entry.getEpoch(), entry.getStartOffset());
            } else {
                return null;
            }
        }

        @Override
        public EpochEntry fromLine(String line) {
            final String[] arr = line.split("-");
            if (arr.length == 2) {
                final int epoch = Integer.parseInt(arr[0]);
                final long startOffset = Long.parseLong(arr[1]);
                return new EpochEntry(epoch, startOffset);
            }
            return null;
        }
    }
}
