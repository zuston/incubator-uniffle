/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.uniffle.shuffle.manager;

import java.util.concurrent.ConcurrentHashMap;

import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.common.util.RssUtils;

public class BlockIdManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(BlockIdManager.class);

  // shuffleId -> partitionId -> blockIds
  private ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Roaring64NavigableMap>> blockIdMap;

  public BlockIdManager() {
    this.blockIdMap = JavaUtils.newConcurrentMap();
  }

  public void addPartitionedBlockIds(int shuffleId, int partitionId, long[] blockIds) {
    ConcurrentHashMap<Integer, Roaring64NavigableMap> partitionedBlockIds =
        blockIdMap.computeIfAbsent(shuffleId, (k) -> JavaUtils.newConcurrentMap());
    partitionedBlockIds
        .computeIfAbsent(partitionId, (j) -> Roaring64NavigableMap.bitmapOf())
        .add(blockIds);
  }

  public Roaring64NavigableMap getPartitionedBlockIds(int shuffleId, int partitionId) {
    ConcurrentHashMap<Integer, Roaring64NavigableMap> partitionedBlockIds =
        blockIdMap.get(shuffleId);
    if (partitionedBlockIds == null || partitionedBlockIds.isEmpty()) {
      return Roaring64NavigableMap.bitmapOf();
    }

    Roaring64NavigableMap idMap = partitionedBlockIds.get(partitionId);
    if (idMap == null || idMap.isEmpty()) {
      return Roaring64NavigableMap.bitmapOf();
    }

    return RssUtils.cloneBitMap(idMap);
  }

  public boolean clearBlockIds(int shuffleId) {
    blockIdMap.remove(shuffleId);
    return true;
  }
}
