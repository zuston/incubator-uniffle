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

package org.apache.uniffle.server.storage.hybrid;

import org.apache.uniffle.server.ShuffleDataFlushEvent;
import org.apache.uniffle.server.storage.AbstractStorageManagerFallbackStrategy;
import org.apache.uniffle.server.storage.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FallbackBasedStorageManagerSelector implements StorageManagerSelector {
  private static final Logger LOGGER = LoggerFactory.getLogger(FallbackBasedStorageManagerSelector.class);

  protected final StorageManager warmStorageManager;
  protected final StorageManager coldStorageManager;
  private final AbstractStorageManagerFallbackStrategy fallbackStrategy;

  public FallbackBasedStorageManagerSelector(
      StorageManager warmStorageManager,
      StorageManager coldStorageManager,
      AbstractStorageManagerFallbackStrategy fallbackStrategy) {
    this.warmStorageManager = warmStorageManager;
    this.coldStorageManager = coldStorageManager;
    this.fallbackStrategy = fallbackStrategy;
  }

  abstract StorageManager regularSelect(ShuffleDataFlushEvent flushEvent);

  private StorageManager fallbackSelect(
      ShuffleDataFlushEvent flushEvent, StorageManager candidateStorageManager) {
    return fallbackStrategy.tryFallback(
        candidateStorageManager, flushEvent, warmStorageManager, coldStorageManager);
  }

  @Override
  public StorageManager select(ShuffleDataFlushEvent flushEvent) {
    StorageManager storageManager = regularSelect(flushEvent);
    if (!storageManager.canWrite(flushEvent) || flushEvent.getRetryTimes() > 0) {
      final StorageManager previous = storageManager;
      storageManager = fallbackSelect(flushEvent, storageManager);
      LOGGER.info("Fallback storage manager from [{}] to [{}] for event: {}",
              previous.getClass().getSimpleName(), storageManager.getClass().getName(), flushEvent);
    }
    return storageManager;
  }
}
