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

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class TimerWheel {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    public static final int BLANK = -1, 
    /**
     * 忽略标记
     */
    IGNORE = -2;
    /**
     * 插槽数量
     */
    public final int slotsTotal;
    /**
     * 时间精度 越小越 准确
     */
    public final int precisionMs;
    /**
     * 文件名
     */
    private String fileName;
    /**
     * 随机访问文件
     */
    private final RandomAccessFile randomAccessFile;
    /**
     * 文件Channel
     */
    private final FileChannel fileChannel;
    /**
     * 文件的映射 ByteBuffer 
     */
    private final MappedByteBuffer mappedByteBuffer;
    /**
     * 直接内存ByteBuffer 将文件内容 写入到该直接内存
     */
    private final ByteBuffer byteBuffer;

    private final ThreadLocal<ByteBuffer> localBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            //为ThreadLocal 创建 文件映射副本
            return byteBuffer.duplicate();
        }
    };
    /**
     * weel 大小 为 插槽的数量 * 插槽的大小 * 2 为什么要 2 倍
     */
    private final int wheelLength;

    public TimerWheel(String fileName, int slotsTotal, int precisionMs) throws IOException {
        this.slotsTotal = slotsTotal;
        this.precisionMs = precisionMs;
        this.fileName = fileName;
        this.wheelLength = this.slotsTotal * 2 * Slot.SIZE;

        File file = new File(fileName);
        //确保文件父级目录
        UtilAll.ensureDirOK(file.getParent());

        try {
            //随机访问文件 文件不为 并且 为 文件大小等于 weel 大小
            randomAccessFile = new RandomAccessFile(this.fileName, "rw");
            if (file.exists() && randomAccessFile.length() != 0 &&
                randomAccessFile.length() != wheelLength) {
                throw new RuntimeException(String.format("Timer wheel length:%d != expected:%s",
                    randomAccessFile.length(), wheelLength));
            }
            //设置文件大小为 wheelLength
            randomAccessFile.setLength(wheelLength);
            fileChannel = randomAccessFile.getChannel();
            //文件进行内存映射
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, wheelLength);
            assert wheelLength == mappedByteBuffer.remaining();
            //分配一个 wheelLength 大小 的直接内存 将 文件内容 写入byteBuffer
            this.byteBuffer = ByteBuffer.allocateDirect(wheelLength);
            this.byteBuffer.put(mappedByteBuffer);
        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        }
    }

    public void shutdown() {
        shutdown(true);
    }

    public void shutdown(boolean flush) {
        //进行刷新
        if (flush)
            this.flush();

        // unmap mappedByteBuffer
        //TODO:: 怎么进行清理的
        UtilAll.cleanBuffer(this.mappedByteBuffer);
        UtilAll.cleanBuffer(this.byteBuffer);

        try {
            this.fileChannel.close();
        } catch (IOException e) {
            log.error("Shutdown error in timer wheel", e);
        }
    }
    /**
     * 获线程当中的 ByteBuffer 直接内存映射的副本
     * 将ByteBuffer 进行比对 修改的内容 写入 文件映射的 ByteBuffer
     * 进行刷新 
     */
    public void flush() {
        //获线程当中的 ByteBuffer 直接内存映射的副本
        ByteBuffer bf = localBuffer.get();
        bf.position(0);
        bf.limit(wheelLength);
        mappedByteBuffer.position(0);
        mappedByteBuffer.limit(wheelLength);
        //将ByteBuffer 进行比对 修改的内容 写入 文件映射的 ByteBuffer 
        for (int i = 0; i < wheelLength; i++) {
            if (bf.get(i) != mappedByteBuffer.get(i)) {
                mappedByteBuffer.put(i, bf.get(i));
            }
        }
        //进行刷新
        this.mappedByteBuffer.force();
    }
    //获取该时间 slot
    public Slot getSlot(long timeMs) {
        Slot slot = getRawSlot(timeMs);
        if (slot.timeMs != timeMs / precisionMs * precisionMs) {
            return new Slot(-1, -1, -1);
        }
        return slot;
    }

    //testable
    public Slot getRawSlot(long timeMs) {
        //定位线程到ByteBuffer对应位置 操作为止
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
        return new Slot(localBuffer.get().getLong() * precisionMs,
            localBuffer.get().getLong(), localBuffer.get().getLong(), localBuffer.get().getInt(), localBuffer.get().getInt());
    }

    public int getSlotIndex(long timeMs) {
        //获取具体 timeMs/precisionMs 获取对应 slot 的位置 index
        return (int) (timeMs / precisionMs % (slotsTotal * 2));
    }

    public void putSlot(long timeMs, long firstPos, long lastPos) {
        //获取具体 timeMs/precisionMs 获取对应 slot 的位置 index
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
        // To be compatible with previous version.
        // The previous version's precision is fixed at 1000ms and it store timeMs / 1000 in slot.
        // timeMs  / precisionMs 时间 对应的 位置
        localBuffer.get().putLong(timeMs / precisionMs);
        localBuffer.get().putLong(firstPos);
        localBuffer.get().putLong(lastPos);
    }
    public void putSlot(long timeMs, long firstPos, long lastPos, int num, int magic) {
        //获取具体 timeMs/precisionMs 获取对应 slot 的位置 index
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
        // timeMs  / precisionMs 时间 对应的 位置
        localBuffer.get().putLong(timeMs / precisionMs);
        localBuffer.get().putLong(firstPos);
        localBuffer.get().putLong(lastPos);
        localBuffer.get().putInt(num);
        localBuffer.get().putInt(magic);
    }

    public void reviseSlot(long timeMs, long firstPos, long lastPos, boolean force) {
        localBuffer.get().position(getSlotIndex(timeMs) * Slot.SIZE);
        //如果该 位置对应的 不是 timeMs / precisionMs 判断是否强制修改
        if (timeMs / precisionMs != localBuffer.get().getLong()) {
            //是否强制修改 进行强制修改
            if (force) {
                putSlot(timeMs, firstPos != IGNORE ? firstPos : lastPos, lastPos);
            }
        } else {
            //如果 firstPos 和 lastPos 是 IGNORE 则不进行修改
            if (IGNORE != firstPos) {
                localBuffer.get().putLong(firstPos);
            } else {
                localBuffer.get().getLong();
            }
            if (IGNORE != lastPos) {
                localBuffer.get().putLong(lastPos);
            }
        }
    }

    //check the timerwheel to see if its stored offset > maxOffset in timerlog
    public long checkPhyPos(long timeStartMs, long maxOffset) {
        long minFirst = Long.MAX_VALUE;
        //根据时间戳 获取对应 slot 的位置 index
        int firstSlotIndex = getSlotIndex(timeStartMs);
        
        for (int i = 0; i < slotsTotal * 2; i++) {
            //从该 slotIndex 位置开始 往后寻找 slotsTotal * 2 的  
            //该时间之后 slotsTotal * 2 * precisionMs 时间
            int slotIndex = (firstSlotIndex + i) % (slotsTotal * 2);
            localBuffer.get().position(slotIndex * Slot.SIZE);
            if ((timeStartMs + i * precisionMs) / precisionMs != localBuffer.get().getLong()) {
                continue;
            }
            long first = localBuffer.get().getLong();
            long last = localBuffer.get().getLong();
            if (last > maxOffset) {
                if (first < minFirst) {
                    minFirst = first;
                }
            }
        }
        return minFirst;
    }

    public long getNum(long timeMs) {
        return getSlot(timeMs).num;
    }

    public long getAllNum(long timeStartMs) {
        int allNum = 0;
        //根据时间戳 获取对应 slot 的位置 index
        int firstSlotIndex = getSlotIndex(timeStartMs);
        for (int i = 0; i < slotsTotal * 2; i++) {
            //从该 slotIndex 位置开始 往后寻找 slotsTotal * 2 的  
            //该时间之后 slotsTotal * 2 * precisionMs 时间的 num 总和
            int slotIndex = (firstSlotIndex + i) % (slotsTotal * 2);
            localBuffer.get().position(slotIndex * Slot.SIZE);
            if ((timeStartMs + i * precisionMs) / precisionMs == localBuffer.get().getLong()) {
                localBuffer.get().getLong(); //first pos
                localBuffer.get().getLong(); //last pos
                allNum = allNum + localBuffer.get().getInt();
            }
        }
        return allNum;
    }
}
