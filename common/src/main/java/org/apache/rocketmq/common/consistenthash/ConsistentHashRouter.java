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
package org.apache.rocketmq.common.consistenthash;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * hash 一致性算法
 * To hash Node objects to a hash ring with a certain amount of virtual node.
 * Method routeNode will return a Node instance which the object key should be allocated to according to consistent hash
 * algorithm
 */
public class ConsistentHashRouter<T extends Node> {
    /**
     * hash 环  key 对应的hash值 value 为对应的虚拟节点
     */
    private final SortedMap<Long, VirtualNode<T>> ring = new TreeMap<Long, VirtualNode<T>>();
    /**
     * hash 函数
     */
    private final HashFunction hashFunction;

    public ConsistentHashRouter(Collection<T> pNodes, int vNodeCount) {
        this(pNodes, vNodeCount, new MD5Hash());
    }

    /**
     * @param pNodes collections of physical nodes
     * @param vNodeCount amounts of virtual nodes
     * @param hashFunction hash Function to hash Node instances
     */
    public ConsistentHashRouter(Collection<T> pNodes, int vNodeCount, HashFunction hashFunction) {
        if (hashFunction == null) {
            throw new NullPointerException("Hash Function is null");
        }
        this.hashFunction = hashFunction;
        if (pNodes != null) {
            for (T pNode : pNodes) {
                addNode(pNode, vNodeCount);
            }
        }
    }

    /**
     * 添加到该物理节点 映射 成 vNodeCount 数量的 虚拟节点 到 hash 环中
     * add physic node to the hash ring with some virtual nodes
     *
     * @param pNode physical node needs added to hash ring
     * @param vNodeCount the number of virtual node of the physical node. Value should be greater than or equals to 0
     */
    public void addNode(T pNode, int vNodeCount) {
        if (vNodeCount < 0)
            throw new IllegalArgumentException("illegal virtual node counts :" + vNodeCount);
        int existingReplicas = getExistingReplicas(pNode);
        for (int i = 0; i < vNodeCount; i++) {
            VirtualNode<T> vNode = new VirtualNode<T>(pNode, i + existingReplicas);
            ring.put(hashFunction.hash(vNode.getKey()), vNode);
        }
    }

    /**
     * 移除该物理节点的虚拟 节点
     * remove the physical node from the hash ring
     */
    public void removeNode(T pNode) {
        Iterator<Long> it = ring.keySet().iterator();
        while (it.hasNext()) {
            Long key = it.next();
            VirtualNode<T> virtualNode = ring.get(key);
            if (virtualNode.isVirtualNodeOf(pNode)) {
                it.remove();
            }
        }
    }

    /**
     * 使用指定的key，在当前哈希环中路由最近的节点实例
     * with a specified key, route the nearest Node instance in the current hash ring
     *
     * @param objectKey the object key to find a nearest Node
     */
    public T routeNode(String objectKey) {
        if (ring.isEmpty()) {
            return null;
        }
        Long hashVal = hashFunction.hash(objectKey);
        //返回比 个值 的后续 Map
        SortedMap<Long, VirtualNode<T>> tailMap = ring.tailMap(hashVal);
        //如果该key后续有最近的节点 则获取该节点 如果没有 则表示 该头 为 最近的节点
        Long nodeHashVal = !tailMap.isEmpty() ? tailMap.firstKey() : ring.firstKey();
        return ring.get(nodeHashVal).getPhysicalNode();
    }

    /**
     * 获取该物理节点的映射的虚拟节点数量
     * @param pNode
     * @return
     */
    public int getExistingReplicas(T pNode) {
        int replicas = 0;
        for (VirtualNode<T> vNode : ring.values()) {
            if (vNode.isVirtualNodeOf(pNode)) {
                replicas++;
            }
        }
        return replicas;
    }

    //default hash function
    private static class MD5Hash implements HashFunction {
        MessageDigest instance;

        public MD5Hash() {
            try {
                instance = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
            }
        }

        @Override
        public long hash(String key) {
            instance.reset();
            instance.update(key.getBytes(StandardCharsets.UTF_8));
            byte[] digest = instance.digest();

            long h = 0;
            for (int i = 0; i < 4; i++) {
                h <<= 8;
                h |= ((int) digest[i]) & 0xFF;
            }
            return h;
        }
    }

}
