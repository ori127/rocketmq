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


import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.logfile.MappedFile;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 多个路径映射文件
 */
public class MultiPathMappedFileQueue extends MappedFileQueue {
    /**
     * 消息存储配置
     */
    private final MessageStoreConfig config;
    /**
     * 已经满的存储路径
     */
    private final Supplier<Set<String>> fullStorePathsSupplier;

    public MultiPathMappedFileQueue(MessageStoreConfig messageStoreConfig, int mappedFileSize,
                                    AllocateMappedFileService allocateMappedFileService,
                                    Supplier<Set<String>> fullStorePathsSupplier) {
        super(messageStoreConfig.getStorePathCommitLog(), mappedFileSize, allocateMappedFileService);
        this.config = messageStoreConfig;
        this.fullStorePathsSupplier = fullStorePathsSupplier;
    }

    /**
     * 获取提交日志存储路径
     * @return
     */
    private Set<String> getPaths() {
        String[] paths = config.getStorePathCommitLog().trim().split(MixAll.MULTI_PATH_SPLITTER);
        return new HashSet<>(Arrays.asList(paths));
    }

    /**
     * 获取提交日志只读存储路径
     * @return
     */
    private Set<String> getReadonlyPaths() {
        String pathStr = config.getReadOnlyCommitLogStorePaths();
        if (StringUtils.isBlank(pathStr)) {
            return Collections.emptySet();
        }
        String[] paths = pathStr.trim().split(MixAll.MULTI_PATH_SPLITTER);
        return new HashSet<>(Arrays.asList(paths));
    }

    @Override
    public boolean load() {
        //获取 存储路径 和 只读存储路径 进行加载
        Set<String> storePathSet = getPaths();
        storePathSet.addAll(getReadonlyPaths());

        List<File> files = new ArrayList<>();
        for (String path : storePathSet) {
            File dir = new File(path);
            File[] ls = dir.listFiles();
            if (ls != null) {
                Collections.addAll(files, ls);
            }
        }

        return doLoad(files);
    }

    @Override
    protected MappedFile tryCreateMappedFile(long createOffset) {
        //创建的偏移量 计算 映射 文件 idx
        long fileIdx = createOffset / this.mappedFileSize;
        //提交日志存储路径
        Set<String> storePath = getPaths();
        //只读提交日志存储路径
        Set<String> readonlyPathSet = getReadonlyPaths();
        Set<String> fullStorePaths =
                fullStorePathsSupplier == null ? Collections.emptySet() : fullStorePathsSupplier.get();


        HashSet<String> availableStorePath = new HashSet<>(storePath);
        //不在只读路径下创建文件 不在空间已经满的路径下创建文件 获取可用的文件路径
        //do not create file in readonly store path.
        availableStorePath.removeAll(readonlyPathSet);

        //do not create file is space is nearly full.
        availableStorePath.removeAll(fullStorePaths);

        //if no store path left, fall back to writable store path.
        //如果没有剩余的存储路径，剩下回到可写的存储路径
        if (availableStorePath.isEmpty()) {
            availableStorePath = new HashSet<>(storePath);
            availableStorePath.removeAll(readonlyPathSet);
        }
        //根据路径进行排序
        String[] paths = availableStorePath.toArray(new String[]{});
        Arrays.sort(paths);
        //进行求余获取 创建文件路基 进行 文件映射
        String nextFilePath = paths[(int) (fileIdx % paths.length)] + File.separator
                + UtilAll.offset2FileName(createOffset);
        String nextNextFilePath = paths[(int) ((fileIdx + 1) % paths.length)] + File.separator
                + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
        return doCreateMappedFile(nextFilePath, nextNextFilePath);
    }

    @Override
    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;


        Set<String> storePathSet = getPaths();
        storePathSet.addAll(getReadonlyPaths());
        //进行删除
        for (String path : storePathSet) {
            File file = new File(path);
            if (file.isDirectory()) {
                file.delete();
            }
        }
    }
}
