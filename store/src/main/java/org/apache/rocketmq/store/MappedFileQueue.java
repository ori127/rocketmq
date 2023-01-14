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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;

public class MappedFileQueue implements Swappable {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);
    /**
     * 队列目录
     */
    protected final String storePath;
    /**
     * 映射文件大小
     */
    protected final int mappedFileSize;
    /**
     * 映射的集合
     */
    protected final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();

    protected final AllocateMappedFileService allocateMappedFileService;
    /**
     * 刷新的位置
     */
    protected long flushedWhere = 0;
    /**
     * 提交的位置
     */
    protected long committedWhere = 0;
    /**
     * 存储的时间
     */
    protected volatile long storeTimestamp = 0;

    public MappedFileQueue(final String storePath, int mappedFileSize,
        AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    /**
     * 遍历 mappedFiles 映射的 文件 判断 映射 文件大小 是否正确
     */
    public void checkSelf() {
        //遍历 mappedFiles 映射的 文件 判断 映射 文件大小 是否正确
        List<MappedFile> mappedFiles = new ArrayList<>(this.mappedFiles);
        if (!mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();

                if (pre != null) {
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
                            pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    /**
     * 获取映射文件 修改时间 大于 这个时间 映射文件 没有则 获取最后一个
     * @param timestamp
     * @return
     */
    public MappedFile getMappedFileByTime(final long timestamp) {
        //拷贝映射的文件
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return null;

        for (int i = 0; i < mfs.length; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            //获取映射文件 修改时间 大于 这个时间 映射文件
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }

        return (MappedFile) mfs[mfs.length - 1];
    }

    /**
     * 拷贝映射的文件
     * @param reservedMappedFiles
     * @return
     */
    protected Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;

        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }

        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    /**
     * 删除 映射 文件开始偏移量 比这个 大的 映射文件 从映射文件中移除
     * @param offset
     */
    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();
        //遍历 mappedFiles
        for (MappedFile file : this.mappedFiles) {
            //映射文件 总的偏移量
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            if (fileTailOffset > offset) {
                //文件的总偏移量 比这个 偏移量 大 ,文件开始偏移量 比这个偏移量 小
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    //文件的总偏移量 比这个 偏移量 大, 文件的开始偏移量 比这个偏移量大 进行删除
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }
        //删除 映射文件关系
        this.deleteExpiredFile(willRemoveFiles);
    }

    /**
     * 移除失效 映射文件文件
     * @param files
     */
    void deleteExpiredFile(List<MappedFile> files) {

        if (!files.isEmpty()) {
            //遍历要 删除的文件 移除 files 不在 映射文件的文件
            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }

            try {
                //从 映射文件集合中移除
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }

    /**
     * 根据存储路径进行加载
     * @return
     */
    public boolean load() {
        File dir = new File(this.storePath);
        File[] ls = dir.listFiles();
        if (ls != null) {
            return doLoad(Arrays.asList(ls));
        }
        return true;
    }

    public boolean doLoad(List<File> files) {
        // ascending order
        //文件名根据偏移量 进行排序
        files.sort(Comparator.comparing(File::getName));
        //遍历目录下的文件
        for (File file : files) {
            if (file.length() != this.mappedFileSize) {
                log.warn(file + "\t" + file.length()
                        + " length not matched message store config value, please check it manually");
                return false;
            }

            try {
                //添加到映射文件当中
                MappedFile mappedFile = new DefaultMappedFile(file.getPath(), mappedFileSize);

                mappedFile.setWrotePosition(this.mappedFileSize);
                mappedFile.setFlushedPosition(this.mappedFileSize);
                mappedFile.setCommittedPosition(this.mappedFileSize);
                this.mappedFiles.add(mappedFile);
                log.info("load " + file.getPath() + " OK");
            } catch (IOException e) {
                log.error("load file " + file + " error", e);
                return false;
            }
        }
        return true;
    }

    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.flushedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }

    /**
     * 获取最后一个文件 ,没有映射文件 存在 则  createOffset 创建文件进行映射
     * @param startOffset
     * @param needCreate
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        //获取最后一个映射文件
        MappedFile mappedFileLast = getLastMappedFile();
        //最后一个映射文件 不存在 则 获取 创建的偏移量
        if (mappedFileLast == null) {
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }
        //若果最后一个文件 已经满了 则 创建的偏移量 为最后一个偏移量 + 文件大小
        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }
        //进行创建
        if (createOffset != -1 && needCreate) {
            return tryCreateMappedFile(createOffset);
        }

        return mappedFileLast;
    }

    public boolean isMappedFilesEmpty() {
        return this.mappedFiles.isEmpty();
    }

    protected MappedFile tryCreateMappedFile(long createOffset) {
        //createOffset 的 文件
        String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
        //该 createOffset 下一个 的文件
        String nextNextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset
                + this.mappedFileSize);
        //创建映射文件
        return doCreateMappedFile(nextFilePath, nextNextFilePath);
    }

    /**
     * 建立文件映射 会创建  nextFilePath 和 nextNextFilePath(可能)
     * @param nextFilePath
     * @param nextNextFilePath
     * @return
     */
    protected MappedFile doCreateMappedFile(String nextFilePath, String nextNextFilePath) {
        MappedFile mappedFile = null;
        //分配映射文件服务 建立文件 映射 会创建  nextFilePath 和 nextNextFilePath
        if (this.allocateMappedFileService != null) {
            mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath,
                    nextNextFilePath, this.mappedFileSize);
        } else {
            try {
                //映射文件
                mappedFile = new DefaultMappedFile(nextFilePath, this.mappedFileSize);
            } catch (IOException e) {
                log.error("create mappedFile exception", e);
            }
        }

        if (mappedFile != null) {
            //该队列的映射文件 为空 则 设置该 文件为 队列里面第一个文件 添加到映射文件集合
            if (this.mappedFiles.isEmpty()) {
                mappedFile.setFirstCreateInQueue(true);
            }
            this.mappedFiles.add(mappedFile);
        }

        return mappedFile;
    }

    /**
     * 获取最后一个文件 ,没有映射文件 存在 则  createOffset 创建文件进行映射
     * @param startOffset
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    /**
     * 获取最后一个映射文件
     * @return
     */
    public MappedFile getLastMappedFile() {
        MappedFile[] mappedFiles = this.mappedFiles.toArray(new MappedFile[0]);
        return mappedFiles.length == 0 ? null : mappedFiles[mappedFiles.length - 1];
    }

    /**
     * 重置该偏移量 找到 该偏移量在 某个 映射文件中 设置 该偏移量 然后 该偏移量只后 映射文件
     * @param offset
     * @return
     */
    public boolean resetOffset(long offset) {
        //获取最后一个映射文件
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {
            //最后的偏移量
            long lastOffset = mappedFileLast.getFileFromOffset() +
                mappedFileLast.getWrotePosition();
            //最后偏移量 和 这个偏移量的差值
            long diff = lastOffset - offset;
            //最大差值为 映射文件 * 2
            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff)
                return false;
        }

        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator(mappedFiles.size());
        //遍历 mappedFiles
        while (iterator.hasPrevious()) {
            mappedFileLast = iterator.previous();
            //查找这个偏移量 的 文件偏移量的 开始偏移量 文件 将该 映射文件设置 该 offset % mappedFileLast.getFileSize() 位置
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                //这个偏移量 只有的文件映射 移除
                iterator.remove();
            }
        }
        return true;
    }

    /**
     * 获取最小的偏移量 该队列映射 文件第一个 映射文件 的偏移量
     * @return
     */
    public long getMinOffset() {

        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }
    /**
     * 获取最大的偏移量 该队列映射 文件最后一个 映射文件 的偏移量 mappedFile.getFileFromOffset() + mappedFile.getReadPosition()
     * @return
     */
    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }
    /**
     * 获取最大的可写偏移量 该队列映射 文件最后一个 映射文件 的偏移量 mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()
     * @return
     */
    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    /**
     * 剩余多少数据量等待提交
     * @return
     */
    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }
    /**
     * 剩余多少数据量等待刷新
     * @return
     */
    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }

    /**
     * 删除最后一个映射文件
     */
    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }

    public int deleteExpiredFileByTime(final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately,
        final int deleteFileBatchMax) {
        //拷贝映射文件集合
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return 0;

        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        //被删除的映射文件
        List<MappedFile> files = new ArrayList<MappedFile>();
        int skipFileNum = 0;
        if (null != mfs) {
            //do check before deleting
            checkSelf();
            //遍历映射文件
            for (int i = 0; i < mfsLength; i++) {
                MappedFile mappedFile = (MappedFile) mfs[i];
                //获取该文件最大存货时间 映射文件最近修改时间 + 过期时间
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
                //如果 当前时间 超过最大存货时间  并且 立即 删除
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    if (skipFileNum > 0) {
                        log.info("Delete CommitLog {} but skip {} files", mappedFile.getFileName(), skipFileNum);
                    }
                    //删除映射文件
                    if (mappedFile.destroy(intervalForcibly)) {
                        files.add(mappedFile);
                        deleteCount++;
                        //超过删除文件 最大批量 跳过
                        if (files.size() >= deleteFileBatchMax) {
                            break;
                        }
                        //等待删除间隔
                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    skipFileNum++;
                    //avoid deleting files in the middle
                    break;
                }
            }
        }
        //移除文件映射
        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     * 删除这个偏移量 之前的文件
     * @param offset
     * @param unitSize
     * @return
     */
    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        //拷贝映射文件集合
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;
            //遍历映射文件
            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                //文件大小 - 1个单位大小 表示该 文件 最后一条消息
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    //获取最后一条 消息的 在队列 中的偏移量 释放 SelectMappedBufferResult
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    //小于这个 偏移量 进行 摧毁
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                            + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }
                //删除映射文件
                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }
        //移除文件映射
        deleteExpiredFile(files);

        return deleteCount;
    }

    public int deleteExpiredFileByOffsetForTimerLog(long offset, int checkOffset, int unitSize) {
        //拷贝映射文件集合
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;
            //遍历映射文件
            for (int i = 0; i < mfsLength; i++) {
                boolean destroy = false;
                MappedFile mappedFile = (MappedFile) mfs[i];
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(checkOffset);
                try {
                    if (result != null) {
                        //TimerLog.UNIT_SIZE
                        int position = result.getByteBuffer().position();
                        int size = result.getByteBuffer().getInt();//size
                        result.getByteBuffer().getLong(); //prev pos
                        int magic = result.getByteBuffer().getInt();
                        if (size == unitSize && (magic | 0xF) == 0xF) {
                            result.getByteBuffer().position(position + MixAll.UNIT_PRE_SIZE_FOR_MSG);
                            //获取该文件的当中最大偏移量 小于 该这个 偏移量 进行删除
                            long maxOffsetPy = result.getByteBuffer().getLong();
                            destroy = maxOffsetPy < offset;
                            if (destroy) {
                                log.info("physic min commitlog offset " + offset + ", current mappedFile's max offset "
                                    + maxOffsetPy + ", delete it");
                            }
                        } else {
                            log.warn("Found error data in [{}] checkOffset:{} unitSize:{}", mappedFile.getFileName(),
                                checkOffset, unitSize);
                        }
                    } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                        log.warn("Found a hanged consume queue file, attempting to delete it.");
                        destroy = true;
                    } else {
                        log.warn("this being not executed forever.");
                        break;
                    }
                } finally {
                    if (null != result) {
                        result.release();
                    }
                }
                //删除映射文件
                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }
        //移除文件映射
        deleteExpiredFile(files);

        return deleteCount;
    }

    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        //根据该偏移量 找到 对应 映射文件 进行刷新 修改存储时间
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            int offset = mappedFile.flush(flushLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.flushedWhere;
            this.flushedWhere = where;
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }

    /**
     * 提交该偏移量对应的映射文件
     * @param commitLeastPages
     * @return
     */
    public synchronized boolean commit(final int commitLeastPages) {
        boolean result = true;
        //根据该偏移量 找到 对应 映射文件 进行提交
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        if (mappedFile != null) {
            int offset = mappedFile.commit(commitLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.committedWhere;
            this.committedWhere = where;
        }

        return result;
    }

    /**
     * 查找在这个偏移量的映射文件
     * Finds a mapped file by offset.
     *
     * @param offset Offset.
     * @param returnFirstOnNotFound If the mapped file is not found, then return the first one.
     * @return Mapped file or null (when not found and returnFirstOnNotFound is <code>false</code>).
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            //获取第一个  和 最后一个 映射文件
            MappedFile firstMappedFile = this.getFirstMappedFile();
            MappedFile lastMappedFile = this.getLastMappedFile();
            if (firstMappedFile != null && lastMappedFile != null) {
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                        offset,
                        firstMappedFile.getFileFromOffset(),
                        lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                        this.mappedFileSize,
                        this.mappedFiles.size());
                } else {
                    //偏移量大于第一个 映射文件的开始 小于 最后一个映射文件的 结束
                    //计算该偏移量 所在该映射文件集合 当中 index
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }
                    //判断该偏移量是 否是在这个 文件中
                    if (targetFile != null && offset >= targetFile.getFileFromOffset()
                        && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }
                    //如果index 不在 那只能 偏移量映射文件去查找
                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset()
                            && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }

                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    /**
     * 获取该队列第一个映射 文件
     * @return
     */
    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    /**
     * 获取该队列 映射内存 的大小
     * @return
     */
    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }

    /**
     * 删除第一个映射文件
     * @param intervalForcibly
     * @return
     */
    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        //获取第一个 映射文件 删除该映射文件 移除该映射关系
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    public void shutdown(final long intervalForcibly) {
        //遍历映射的文件进行关闭
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    /**
     * 关闭文件进行删除 删除 父类 目录
     */
    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    @Override
    public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {

        if (mappedFiles.isEmpty()) {
            return;
        }

        if (reserveNum < 3) {
            reserveNum = 3;
        }

        Object[] mfs = this.copyMappedFiles(0);
        if (null == mfs) {
            return;
        }

        for (int i = mfs.length - reserveNum - 1; i >= 0; i--) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            //超过强制交换 进行交换
            if (System.currentTimeMillis() - mappedFile.getRecentSwapMapTime() > forceSwapIntervalMs) {
                mappedFile.swapMap();
                continue;
            }
            //超过 正常交换间隔 进行交换
            if (System.currentTimeMillis() - mappedFile.getRecentSwapMapTime() > normalSwapIntervalMs
                    && mappedFile.getMappedByteBufferAccessCountSinceLastSwap() > 0) {
                mappedFile.swapMap();
                continue;
            }
        }
    }

    /**
     * 清理映射 文件的 交换缓存
     * @param forceCleanSwapIntervalMs
     */
    @Override
    public void cleanSwappedMap(long forceCleanSwapIntervalMs) {

        if (mappedFiles.isEmpty()) {
            return;
        }

        int reserveNum = 3;
        Object[] mfs = this.copyMappedFiles(0);
        if (null == mfs) {
            return;
        }

        for (int i = mfs.length - reserveNum - 1; i >= 0; i--) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            ///超过强制清理交换间隔进行 清理
            if (System.currentTimeMillis() - mappedFile.getRecentSwapMapTime() > forceCleanSwapIntervalMs) {
                mappedFile.cleanSwapedMap(false);
            }
        }
    }

    public Object[] snapshot() {
        // return a safe copy
        return this.mappedFiles.toArray();
    }

    public Stream<MappedFile> stream() {
        return this.mappedFiles.stream();
    }

    public Stream<MappedFile> reversedStream() {
        return Lists.reverse(this.mappedFiles).stream();
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }

    /**
     * 总文件大小
     * @return
     */
    public long getTotalFileSize() {
        return (long) mappedFileSize * mappedFiles.size();
    }
}
