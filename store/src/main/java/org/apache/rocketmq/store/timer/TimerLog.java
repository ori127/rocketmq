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
package org.apache.rocketmq.store.timer;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.MappedFileQueue;
import org.apache.rocketmq.store.SelectMappedBufferResult;

import java.nio.ByteBuffer;

public class TimerLog {
    private static InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    public final static int BLANK_MAGIC_CODE = 0xBBCCDDEE ^ 1880681586 + 8;
    /**
     * 最小空白
     */
    private final static int MIN_BLANK_LEN = 4 + 8 + 4;
    /**
     * 52个字节
     */
    public final static int UNIT_SIZE = 4  //size
            + 8 //prev pos
            + 4 //magic value
            + 8 //curr write time, for trace
            + 4 //delayed time, for check
            + 8 //offsetPy
            + 4 //sizePy
            + 4 //hash code of real topic
            + 8; //reserved value, just in case of
    public final static int UNIT_PRE_SIZE_FOR_MSG = 28;
    public final static int UNIT_PRE_SIZE_FOR_METRIC = 40;
    private final MappedFileQueue mappedFileQueue;

    private final int fileSize;

    public TimerLog(final String storePath, final int fileSize) {
        this.fileSize = fileSize;
        this.mappedFileQueue = new MappedFileQueue(storePath, fileSize, null);
    }

    public boolean load() {
        return this.mappedFileQueue.load();
    }

    public long append(byte[] data) {
        return append(data, 0, data.length);
    }

    public long append(byte[] data, int pos, int len) {
        //获取最后一个映射文件 不存在则 进行创建
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (null == mappedFile || mappedFile.isFull()) {
            mappedFile = this.mappedFileQueue.getLastMappedFile(0);
        }
        if (null == mappedFile) {
            log.error("Create mapped file1 error for timer log");
            return -1;
        }
        //如果 剩余 文件大小 小于 数据长度 + 最小空白长度 则将该文件填上 最小空白长度
        if (len + MIN_BLANK_LEN > mappedFile.getFileSize() - mappedFile.getWrotePosition()) {
            //最小空白 16 个字节
            ByteBuffer byteBuffer = ByteBuffer.allocate(MIN_BLANK_LEN);
            byteBuffer.putInt(mappedFile.getFileSize() - mappedFile.getWrotePosition());
            byteBuffer.putLong(0);
            byteBuffer.putInt(BLANK_MAGIC_CODE);
            if (mappedFile.appendMessage(byteBuffer.array())) {
                //need to set the wrote position
                //可能 还有 空余 位置 将写位置 设置 成文件大小 可以 保证 重新获取 最后一个 文件  会创建新的文件
                mappedFile.setWrotePosition(mappedFile.getFileSize());
            } else {
                log.error("Append blank error for timer log");
                return -1;
            }
            //重新获取 最后一个 文件  会创建新的文件
            mappedFile = this.mappedFileQueue.getLastMappedFile(0);
            if (null == mappedFile) {
                log.error("create mapped file2 error for timer log");
                return -1;
            }
        }
        //获取当期映射文件位置 写入消息
        long currPosition = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        if (!mappedFile.appendMessage(data, pos, len)) {
            log.error("Append error for timer log");
            return -1;
        }
        return currPosition;
    }

    /**
     * 根据偏移量找对应的映射文件 然后 设置该 文件偏移量
     * @param offsetPy
     * @return
     */
    public SelectMappedBufferResult getTimerMessage(long offsetPy) {
        //根据偏移量找对应的映射文件 然后 设置该 文件偏移量
        MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(offsetPy);
        if (null == mappedFile)
            return null;
        return mappedFile.selectMappedBuffer((int) (offsetPy % mappedFile.getFileSize()));
    }

    public SelectMappedBufferResult getWholeBuffer(long offsetPy) {
        MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(offsetPy);
        if (null == mappedFile)
            return null;
        return mappedFile.selectMappedBuffer(0);
    }

    public MappedFileQueue getMappedFileQueue() {
        return mappedFileQueue;
    }

    public void shutdown() {
        this.mappedFileQueue.flush(0);
        //it seems do not need to call shutdown
    }

    // be careful.
    // if the format of timerlog changed, this offset has to be changed too
    // so dose the batch writing
    public int getOffsetForLastUnit() {
        // fileSize - (MIN_BLANK_LEN -UNIT_SIZE) 68  -  (fileSize - MIN_BLANK_LEN) % UNIT_SIZE 空白大小
        return fileSize - (fileSize - MIN_BLANK_LEN) % UNIT_SIZE - MIN_BLANK_LEN - UNIT_SIZE;
    }

}
