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

package org.apache.rocketmq.store.ha.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class HAWriter {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    protected final List<HAWriteHook> writeHookList = new ArrayList<>();

    public boolean write(SocketChannel socketChannel, ByteBuffer byteBufferWrite) throws IOException {
        //连续写入字节数量 为 0的次数
        int writeSizeZeroTimes = 0;
        //如果还有剩余 则向 该通道写入字节 调用写 后钩子
        while (byteBufferWrite.hasRemaining()) {
            int writeSize = socketChannel.write(byteBufferWrite);
            for (HAWriteHook writeHook : writeHookList) {
                writeHook.afterWrite(writeSize);
            }
            //如果有字节写出 则 重新计数
            if (writeSize > 0) {
                writeSizeZeroTimes = 0;
            } else if (writeSize == 0) {
                //如果连续写入字节为0的次数超过3次则退出
                if (++writeSizeZeroTimes >= 3) {
                    break;
                }
            } else {
                LOGGER.info("Write socket < 0");
            }
        }

        return !byteBufferWrite.hasRemaining();
    }

    public void registerHook(HAWriteHook writeHook) {
        writeHookList.add(writeHook);
    }

    public void clearHook() {
        writeHookList.clear();
    }
}
