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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.common.AbstractBrokerRunnable;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

public class IndexService {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * 创建 index 文件 最大尝试次数
     * Maximum times to attempt index file creation.
     */
    private static final int MAX_TRY_IDX_CREATE = 3;
    private final DefaultMessageStore defaultMessageStore;
    /**
     * hash slot 的数量
     */
    private final int hashSlotNum;
    /**
     * index 的数量
     */
    private final int indexNum;
    /***
     * 存储路径 "/index"
     */
    private final String storePath;
    private final ArrayList<IndexFile> indexFileList = new ArrayList<IndexFile>();
    /**
     * 锁
     */
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public IndexService(final DefaultMessageStore store) {
        this.defaultMessageStore = store;
        this.hashSlotNum = store.getMessageStoreConfig().getMaxHashSlotNum();
        this.indexNum = store.getMessageStoreConfig().getMaxIndexNum();
        this.storePath =
            StorePathConfigHelper.getStorePathIndex(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
    }

    public boolean load(final boolean lastExitOK) {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {
                try {
                    IndexFile f = new IndexFile(file.getPath(), this.hashSlotNum, this.indexNum, 0, 0);
                    f.load();

                    if (!lastExitOK) {
                        //索引文件超过检查点 记录时间 index 对文件进行销毁 删除
                        if (f.getEndTimestamp() > this.defaultMessageStore.getStoreCheckpoint()
                            .getIndexMsgTimestamp()) {
                            f.destroy(0);
                            continue;
                        }
                    }

                    LOGGER.info("load index file OK, " + f.getFileName());
                    this.indexFileList.add(f);
                } catch (IOException e) {
                    LOGGER.error("load file {} error", file, e);
                    return false;
                } catch (NumberFormatException e) {
                    LOGGER.error("load file {} error", file, e);
                }
            }
        }

        return true;
    }

    /**
     * index 文件总共 大小
     * @return
     */
    public long getTotalSize() {
        if (indexFileList.isEmpty()) {
            return 0;
        }

        return (long) indexFileList.get(0).getFileSize() * indexFileList.size();
    }

    public void deleteExpiredFile(long offset) {
        Object[] files = null;
        try {
            this.readWriteLock.readLock().lock();
            if (this.indexFileList.isEmpty()) {
                return;
            }
            //判断最后一个index文件结束偏移量是否比这个 偏移量小
            long endPhyOffset = this.indexFileList.get(0).getEndPhyOffset();
            if (endPhyOffset < offset) {
                files = this.indexFileList.toArray();
            }
        } catch (Exception e) {
            LOGGER.error("destroy exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        if (files != null) {
            //遍历 index文件集合 删除 结束遍历偏移量小于该偏移量的 index文件
            List<IndexFile> fileList = new ArrayList<IndexFile>();
            for (int i = 0; i < (files.length - 1); i++) {
                IndexFile f = (IndexFile) files[i];
                if (f.getEndPhyOffset() < offset) {
                    fileList.add(f);
                } else {
                    break;
                }
            }

            this.deleteExpiredFile(fileList);
        }
    }

    private void deleteExpiredFile(List<IndexFile> files) {
        if (!files.isEmpty()) {
            try {
                this.readWriteLock.writeLock().lock();
                //遍历index文件 进行删除 从index文件集合 中移除
                for (IndexFile file : files) {
                    boolean destroyed = file.destroy(3000);
                    destroyed = destroyed && this.indexFileList.remove(file);
                    if (!destroyed) {
                        LOGGER.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            } catch (Exception e) {
                LOGGER.error("deleteExpiredFile has exception.", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
    }

    public void destroy() {
        try {
            this.readWriteLock.writeLock().lock();
            for (IndexFile f : this.indexFileList) {
                f.destroy(1000 * 3);
            }
            this.indexFileList.clear();
        } catch (Exception e) {
            LOGGER.error("destroy exception", e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }
    }

    public QueryOffsetResult queryOffset(String topic, String key, int maxNum, long begin, long end) {
        List<Long> phyOffsets = new ArrayList<Long>(maxNum);

        long indexLastUpdateTimestamp = 0;
        long indexLastUpdatePhyoffset = 0;
        maxNum = Math.min(maxNum, this.defaultMessageStore.getMessageStoreConfig().getMaxMsgsNumBatch());
        try {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                for (int i = this.indexFileList.size(); i > 0; i--) {
                    IndexFile f = this.indexFileList.get(i - 1);
                    boolean lastFile = i == this.indexFileList.size();
                    //记录最后一个index文件的更新时间 和 结束偏移量
                    if (lastFile) {
                        indexLastUpdateTimestamp = f.getEndTimestamp();
                        indexLastUpdatePhyoffset = f.getEndPhyOffset();
                    }
                    //index文件是否符合这个时间 然后查找 key 为 "topic#key"
                    if (f.isTimeMatched(begin, end)) {

                        f.selectPhyOffset(phyOffsets, buildKey(topic, key), maxNum, begin, end);
                    }

                    if (f.getBeginTimestamp() < begin) {
                        break;
                    }

                    if (phyOffsets.size() >= maxNum) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("queryMsg exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return new QueryOffsetResult(phyOffsets, indexLastUpdateTimestamp, indexLastUpdatePhyoffset);
    }

    private String buildKey(final String topic, final String key) {
        return topic + "#" + key;
    }

    public void buildIndex(DispatchRequest req) {
        //获取最后一个或者创建index 文件
        IndexFile indexFile = retryGetAndCreateIndexFile();
        if (indexFile != null) {
            long endPhyOffset = indexFile.getEndPhyOffset();
            DispatchRequest msg = req;
            String topic = msg.getTopic();
            String keys = msg.getKeys();
            //消息的提交日志偏移量小于 结束偏移量的 则不构建index
            if (msg.getCommitLogOffset() < endPhyOffset) {
                return;
            }
            //获取事务标记
            final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    break;
                //事务回滚不构建index
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    return;
            }

            if (req.getUniqKey() != null) {
                //存储唯一key 则建立 index
                indexFile = putKey(indexFile, msg, buildKey(topic, req.getUniqKey()));
                if (indexFile == null) {
                    LOGGER.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                    return;
                }
            }

            if (keys != null && keys.length() > 0) {
                //keys 以 " " 进行分割 为每个key建立索引
                String[] keyset = keys.split(MessageConst.KEY_SEPARATOR);
                for (int i = 0; i < keyset.length; i++) {
                    String key = keyset[i];
                    if (key.length() > 0) {
                        indexFile = putKey(indexFile, msg, buildKey(topic, key));
                        if (indexFile == null) {
                            LOGGER.error("putKey error commitlog {} uniqkey {}", req.getCommitLogOffset(), req.getUniqKey());
                            return;
                        }
                    }
                }
            }
        } else {
            LOGGER.error("build index error, stop building index");
        }
    }

    private IndexFile putKey(IndexFile indexFile, DispatchRequest msg, String idxKey) {
        //在index 文件上建立 index 直至成功
        for (boolean ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp()); !ok; ) {
            LOGGER.warn("Index file [" + indexFile.getFileName() + "] is full, trying to create another one");

            indexFile = retryGetAndCreateIndexFile();
            if (null == indexFile) {
                return null;
            }

            ok = indexFile.putKey(idxKey, msg.getCommitLogOffset(), msg.getStoreTimestamp());
        }

        return indexFile;
    }

    /**
     * 获取或者创建index 文件
     * Retries to get or create index file.
     *
     * @return {@link IndexFile} or null on failure.
     */
    public IndexFile retryGetAndCreateIndexFile() {
        IndexFile indexFile = null;
        //重试获取 index 文件或 创建index文件
        for (int times = 0; null == indexFile && times < MAX_TRY_IDX_CREATE; times++) {
            //获取或者创建一个最后index文件
            indexFile = this.getAndCreateLastIndexFile();
            if (null != indexFile) {
                break;
            }

            try {
                LOGGER.info("Tried to create index file " + times + " times");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted", e);
            }
        }

        if (null == indexFile) {
            this.defaultMessageStore.getRunningFlags().makeIndexFileError();
            LOGGER.error("Mark index file cannot build flag");
        }

        return indexFile;
    }

    public IndexFile getAndCreateLastIndexFile() {
        IndexFile indexFile = null;
        IndexFile prevIndexFile = null;
        long lastUpdateEndPhyOffset = 0;
        long lastUpdateIndexTimestamp = 0;

        {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                //如果 index文件集合不为空 则获取最后一个index文件
                IndexFile tmp = this.indexFileList.get(this.indexFileList.size() - 1);
                if (!tmp.isWriteFull()) {
                    indexFile = tmp;
                } else {
                    //最后一个index文件满了 则记录 结束的物理偏移量 和 结束的时间  prevIndexFile指向该文件
                    lastUpdateEndPhyOffset = tmp.getEndPhyOffset();
                    lastUpdateIndexTimestamp = tmp.getEndTimestamp();
                    prevIndexFile = tmp;
                }
            }

            this.readWriteLock.readLock().unlock();
        }

        if (indexFile == null) {
            try {
                String fileName =
                    this.storePath + File.separator
                        + UtilAll.timeMillisToHumanString(System.currentTimeMillis());
                //以上一个文件的结束的物理偏移量 为 该文件开始偏移量  和  以上一个文件的结束的时间 为文件的开始时间 创建文件 添加index文件当中
                indexFile =
                    new IndexFile(fileName, this.hashSlotNum, this.indexNum, lastUpdateEndPhyOffset,
                        lastUpdateIndexTimestamp);
                this.readWriteLock.writeLock().lock();
                this.indexFileList.add(indexFile);
            } catch (Exception e) {
                LOGGER.error("getLastIndexFile exception ", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }

            if (indexFile != null) {
                final IndexFile flushThisFile = prevIndexFile;
                //创建一个后台线程进行已经满的index 文件
                Thread flushThread = new Thread(new AbstractBrokerRunnable(defaultMessageStore.getBrokerConfig()) {
                    @Override
                    public void run2() {
                        IndexService.this.flush(flushThisFile);
                    }
                }, "FlushIndexFileThread");

                flushThread.setDaemon(true);
                flushThread.start();
            }
        }

        return indexFile;
    }

    public void flush(final IndexFile f) {
        if (null == f) {
            return;
        }

        long indexMsgTimestamp = 0;
        //文件如果 满了记录 index消息时间戳
        if (f.isWriteFull()) {
            indexMsgTimestamp = f.getEndTimestamp();
        }
        //文件刷新 到磁盘
        f.flush();
        //检查点 记录 index消息的时间
        if (indexMsgTimestamp > 0) {
            this.defaultMessageStore.getStoreCheckpoint().setIndexMsgTimestamp(indexMsgTimestamp);
            this.defaultMessageStore.getStoreCheckpoint().flush();
        }
    }

    public void start() {

    }

    /***
     * 关闭index文件集合
     */
    public void shutdown() {
        try {
            this.readWriteLock.writeLock().lock();
            for (IndexFile f : this.indexFileList) {
                try {
                    f.shutdown();
                } catch (Exception e) {
                    LOGGER.error("shutdown " + f.getFileName() + " exception", e);
                }
            }
            this.indexFileList.clear();
        } catch (Exception e) {
            LOGGER.error("shutdown exception", e);
        } finally {
            this.readWriteLock.writeLock().unlock();
        }
    }
}
