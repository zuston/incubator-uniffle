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

package org.apache.uniffle.storage.common;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.storage.handler.api.ServerReadHandler;
import org.apache.uniffle.storage.handler.api.ShuffleWriteHandler;
import org.apache.uniffle.storage.request.CreateShuffleReadHandlerRequest;
import org.apache.uniffle.storage.request.CreateShuffleWriteHandlerRequest;

public class ChainableLocalStorage extends AbstractStorage {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChainableLocalStorage.class);

  private final List<LocalStorage> chainableStorages;

  public ChainableLocalStorage(@Nonnull LocalStorage localStorage) {
    this.chainableStorages = new ArrayList<>();
    chainableStorages.add(localStorage);
  }

  public String getBasePath() {
    throw new RuntimeException("isCorrupted is not supported");
  }

  public boolean isCorrupted() {
    throw new RuntimeException("isCorrupted is not supported");
  }

  @Override
  ShuffleWriteHandler newWriteHandler(CreateShuffleWriteHandlerRequest request) {
    throw new RuntimeException("newWriteHandler is not supported");
  }

  @Override
  public ShuffleWriteHandler getOrCreateWriteHandler(CreateShuffleWriteHandlerRequest request) {
    throw new RuntimeException("getOrCreateWriteHandler is not supported");
  }

  @Override
  public ServerReadHandler getOrCreateReadHandler(CreateShuffleReadHandlerRequest request) {
    int index = request.getStorageSeqIndex();
    return chainableStorages.get(index).getOrCreateReadHandler(request);
  }

  @Override
  protected ServerReadHandler newReadHandler(CreateShuffleReadHandlerRequest request) {
    throw new RuntimeException("newReadHandler is not supported in " + this.getClass().getSimpleName());
  }

  @Override
  public boolean canWrite() {
    throw new RuntimeException("canWrite is not supported in " + this.getClass().getSimpleName());
  }

  @Override
  public boolean lockShuffleShared(String shuffleKey) {
    throw new RuntimeException("lockShuffleShared is not supported in " + this.getClass().getSimpleName());
  }

  @Override
  public boolean unlockShuffleShared(String shuffleKey) {
    throw new RuntimeException("unlockShuffleShared is not supported in " + this.getClass().getSimpleName());
  }

  @Override
  public boolean lockShuffleExcluded(String shuffleKey) {
    return false;
  }

  @Override
  public boolean unlockShuffleExcluded(String shuffleKey) {
    return false;
  }

  @Override
  public void updateWriteMetrics(StorageWriteMetrics metrics) {
    throw new RuntimeException("updateWriteMetrics is not supported in " + this.getClass().getSimpleName());
  }

  @Override
  public void updateReadMetrics(StorageReadMetrics metrics) {
    throw new RuntimeException("updateReadMetrics is not supported in " + this.getClass().getSimpleName());
  }

  @Override
  public void createMetadataIfNotExist(String shuffleKey) {
    throw new RuntimeException("createMetadataIfNotExist is not supported in " + this.getClass().getSimpleName());
  }

  @Override
  public String getStoragePath() {
    throw new RuntimeException("getStoragePath is not supported in " + this.getClass().getSimpleName());
  }

  @Override
  public String getStorageHost() {
    throw new RuntimeException("getStorageHost is not supported in " + this.getClass().getSimpleName());
  }

  public void removeTailStorage() {
    this.chainableStorages.remove(this.chainableStorages.size() - 1);
  }

  public List<LocalStorage> getChainableStorages() {
    return chainableStorages;
  }

  public void switchTo(LocalStorage newLocalStorage) {
    // If it's the used storage, it should be switched as the latest local storage to write.
    if (chainableStorages.contains(newLocalStorage)) {
      chainableStorages.remove(newLocalStorage);
    }
    chainableStorages.add(newLocalStorage);
  }
}
