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

package org.apache.spark.shuffle;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.proto.RssProtos;

/**
 * Class for holding, 1. partition ID -> shuffle servers mapping. 2. remote storage info
 *
 * <p>It's to be broadcast to executors and referenced by shuffle tasks.
 */
public class ShuffleHandleInfo implements Serializable {

  private int shuffleId;
  private RemoteStorageInfo remoteStorage;

  /**
   * partitionId -> replica -> assigned servers.
   *
   * <p>The first index of list<ShuffleServerInfo> is the initial static assignment server.
   *
   * <p>The remaining indexes are the replacement servers if exists.
   */
  private Map<Integer, Map<Integer, List<ShuffleServerInfo>>> partitionReplicaAssignedServers;
  // excluded server's replacements  mapping
  private Map<String, Set<ShuffleServerInfo>> excludedServerToReplacements;
  // The collection of partition ids that need to be load balanced, such as huge partition.
  private Set<Integer> loadBalancePartitionIds = new HashSet<>();

  public static final ShuffleHandleInfo EMPTY_HANDLE_INFO =
      new ShuffleHandleInfo(-1, Collections.EMPTY_MAP, RemoteStorageInfo.EMPTY_REMOTE_STORAGE);

  public ShuffleHandleInfo(
      int shuffleId,
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      RemoteStorageInfo storageInfo) {
    this.shuffleId = shuffleId;
    this.remoteStorage = storageInfo;
    this.excludedServerToReplacements = new HashMap<>();
    this.partitionReplicaAssignedServers = toPartitionReplicaMapping(partitionToServers);
  }

  @VisibleForTesting
  protected ShuffleHandleInfo(
      int shuffleId,
      RemoteStorageInfo storageInfo,
      Map<Integer, Map<Integer, List<ShuffleServerInfo>>> partitionReplicaAssignedServers) {
    this.shuffleId = shuffleId;
    this.remoteStorage = storageInfo;
    this.excludedServerToReplacements = new HashMap<>();
    this.partitionReplicaAssignedServers = partitionReplicaAssignedServers;
  }

  public ShuffleHandleInfo() {
    // ignore
  }

  private Map<Integer, Map<Integer, List<ShuffleServerInfo>>> toPartitionReplicaMapping(
      Map<Integer, List<ShuffleServerInfo>> partitionToServers) {
    Map<Integer, Map<Integer, List<ShuffleServerInfo>>> partitionReplicaAssignedServers =
        new HashMap<>();
    for (Map.Entry<Integer, List<ShuffleServerInfo>> partitionEntry :
        partitionToServers.entrySet()) {
      int partitionId = partitionEntry.getKey();
      Map<Integer, List<ShuffleServerInfo>> replicaMapping =
          partitionReplicaAssignedServers.computeIfAbsent(partitionId, x -> new HashMap<>());

      List<ShuffleServerInfo> replicaServers = partitionEntry.getValue();
      for (int i = 0; i < replicaServers.size(); i++) {
        int replicaIdx = i;
        replicaMapping
            .computeIfAbsent(replicaIdx, x -> new ArrayList<>())
            .add(replicaServers.get(i));
      }
    }
    return partitionReplicaAssignedServers;
  }

  /**
   * This composes the partition's replica servers + replacement servers, this will be used by the
   * shuffleReader to get the blockIds
   */
  public Map<Integer, List<ShuffleServerInfo>> listPartitionAssignedServers() {
    Map<Integer, List<ShuffleServerInfo>> partitionServers = new HashMap<>();
    for (Map.Entry<Integer, Map<Integer, List<ShuffleServerInfo>>> entry :
        partitionReplicaAssignedServers.entrySet()) {
      int partitionId = entry.getKey();
      Map<Integer, List<ShuffleServerInfo>> replicaServers = entry.getValue();
      List<ShuffleServerInfo> servers =
          replicaServers.values().stream().flatMap(x -> x.stream()).collect(Collectors.toList());
      partitionServers.computeIfAbsent(partitionId, x -> new ArrayList<>()).addAll(servers);
    }
    return partitionServers;
  }

  /** Return all the assigned servers for the writer to commit */
  public Set<ShuffleServerInfo> listAssignedServers() {
    return partitionReplicaAssignedServers.values().stream()
        .flatMap(x -> x.values().stream())
        .flatMap(x -> x.stream())
        .collect(Collectors.toSet());
  }

  public RemoteStorageInfo getRemoteStorage() {
    return remoteStorage;
  }

  public int getShuffleId() {
    return shuffleId;
  }

  @VisibleForTesting
  protected boolean isMarkedAsFaultyServer(String serverId) {
    return excludedServerToReplacements.containsKey(serverId);
  }

  public Set<ShuffleServerInfo> getReplacements(String faultyServerId) {
    return excludedServerToReplacements.get(faultyServerId);
  }

  public Map<Integer, Set<ShuffleServerInfo>> updateAssignment(
      Set<Integer> partitionIds, String faultyServerId, Set<ShuffleServerInfo> replacements) {
    return updateAssignment(partitionIds, faultyServerId, replacements, new HashSet<>());
  }

  public void updateLoadBalancePartitions(List<Integer> partitionIds) {
    partitionIds.stream().forEach(x -> loadBalancePartitionIds.add(x));
  }

  public Set<ShuffleServerInfo> updateAssignment(
      int partitionId, String receivingFailureServerId, Set<ShuffleServerInfo> replacements) {
    if (replacements == null || StringUtils.isEmpty(receivingFailureServerId)) {
      return Collections.emptySet();
    }
    excludedServerToReplacements.put(receivingFailureServerId, replacements);

    Set<ShuffleServerInfo> updatedServers = new HashSet<>();
    Map<Integer, List<ShuffleServerInfo>> replicaServers =
        partitionReplicaAssignedServers.get(partitionId);
    for (Map.Entry<Integer, List<ShuffleServerInfo>> serverEntry : replicaServers.entrySet()) {
      List<ShuffleServerInfo> servers = serverEntry.getValue();
      if (servers.stream()
          .map(x -> x.getId())
          .collect(Collectors.toSet())
          .contains(receivingFailureServerId)) {
        Set<ShuffleServerInfo> tempSet = new HashSet<>();
        tempSet.addAll(replacements);
        tempSet.removeAll(servers);

        if (CollectionUtils.isNotEmpty(tempSet)) {
          updatedServers.addAll(tempSet);
          servers.addAll(tempSet);
        }
      }
    }
    return updatedServers;
  }

  /**
   * Update the assignment for partitions by replacement servers
   *
   * @return the updated partition servers collections successfully
   */
  public Map<Integer, Set<ShuffleServerInfo>> updateAssignment(
      Set<Integer> partitionIds,
      String faultyServerId,
      Set<ShuffleServerInfo> replacements,
      Set<Integer> loadBalancePartitionIds) {
    if (replacements == null) {
      return Collections.emptyMap();
    }
    excludedServerToReplacements.put(faultyServerId, replacements);

    if (CollectionUtils.isNotEmpty(loadBalancePartitionIds)) {
      this.loadBalancePartitionIds.addAll(loadBalancePartitionIds);
    }

    Map<Integer, Set<ShuffleServerInfo>> updatedPartitionServers = new HashMap<>();

    // todo: optimize the multiple for performance
    for (Integer partitionId : partitionIds) {
      Map<Integer, List<ShuffleServerInfo>> replicaServers =
          partitionReplicaAssignedServers.get(partitionId);
      for (Map.Entry<Integer, List<ShuffleServerInfo>> serverEntry : replicaServers.entrySet()) {
        List<ShuffleServerInfo> servers = serverEntry.getValue();
        if (servers.stream()
            .map(x -> x.getId())
            .collect(Collectors.toSet())
            .contains(faultyServerId)) {
          Set<ShuffleServerInfo> tempSet = new HashSet<>();
          tempSet.addAll(replacements);
          tempSet.removeAll(servers);

          if (CollectionUtils.isNotEmpty(tempSet)) {
            updatedPartitionServers
                .computeIfAbsent(partitionId, x -> new HashSet<>())
                .addAll(tempSet);

            servers.addAll(tempSet);
          }
        }
      }
    }
    return updatedPartitionServers;
  }

  // partitionId -> replica -> failover servers
  // always return the last server.
  @VisibleForTesting
  public Map<Integer, List<ShuffleServerInfo>> getPartitionToServers() {
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = new HashMap<>();
    for (Map.Entry<Integer, Map<Integer, List<ShuffleServerInfo>>> entry :
        partitionReplicaAssignedServers.entrySet()) {
      int partitionId = entry.getKey();
      Map<Integer, List<ShuffleServerInfo>> replicaServers = entry.getValue();
      for (Map.Entry<Integer, List<ShuffleServerInfo>> replicaServerEntry :
          replicaServers.entrySet()) {
        ShuffleServerInfo lastServer =
            replicaServerEntry.getValue().get(replicaServerEntry.getValue().size() - 1);
        partitionToServers.computeIfAbsent(partitionId, x -> new ArrayList<>()).add(lastServer);
      }
    }
    return partitionToServers;
  }

  /**
   * Leveraging the partition reassign mechanism, it could support multiple servers for one
   * partition replica for huge partition load balance or reassignment multiple times. But it will
   * use the different policies.
   *
   * <p>For the former, this will use the hash to get one from the candidates. For the latter, this
   * will always get the last one that is available for now.
   *
   * @param taskAttemptId
   * @return the latest mapping of partition writing to servers for the task. key: partitionId,
   *     value: the servers for multiple replicas.
   */
  public Map<Integer, List<ShuffleServerInfo>> getLatestAssignmentByTaskAttemptId(
      long taskAttemptId) {
    Map<Integer, List<ShuffleServerInfo>> assignment = new HashMap<>();
    for (Map.Entry<Integer, Map<Integer, List<ShuffleServerInfo>>> entry :
        partitionReplicaAssignedServers.entrySet()) {
      int partitionId = entry.getKey();
      boolean isNeedLoadBalance = loadBalancePartitionIds.contains(partitionId);
      Map<Integer, List<ShuffleServerInfo>> replicaServers = entry.getValue();
      for (Map.Entry<Integer, List<ShuffleServerInfo>> replicaServerEntry :
          replicaServers.entrySet()) {
        ShuffleServerInfo candidate;
        int candidateSize = replicaServerEntry.getValue().size();
        // todo: loop find the next candidate if current candidate is in faulty list.
        if (isNeedLoadBalance) {
          candidate = replicaServerEntry.getValue().get((int) (taskAttemptId % candidateSize));
        } else {
          candidate = replicaServerEntry.getValue().get(candidateSize - 1);
        }
        assignment.computeIfAbsent(partitionId, x -> new ArrayList<>()).add(candidate);
      }
    }
    return assignment;
  }

  public PartitionDataReplicaRequirementTracking createPartitionReplicaTracking() {
    PartitionDataReplicaRequirementTracking replicaRequirement =
        new PartitionDataReplicaRequirementTracking(shuffleId, partitionReplicaAssignedServers);
    return replicaRequirement;
  }

  public static RssProtos.ShuffleHandleInfo toProto(ShuffleHandleInfo handleInfo) {
    synchronized (handleInfo) {
      Map<Integer, RssProtos.PartitionReplicaServers> partitionToServers = new HashMap<>();
      for (Map.Entry<Integer, Map<Integer, List<ShuffleServerInfo>>> entry :
          handleInfo.partitionReplicaAssignedServers.entrySet()) {
        int partitionId = entry.getKey();

        Map<Integer, RssProtos.ReplicaServersItem> replicaServersProto = new HashMap<>();
        Map<Integer, List<ShuffleServerInfo>> replicaServers = entry.getValue();
        for (Map.Entry<Integer, List<ShuffleServerInfo>> replicaServerEntry :
            replicaServers.entrySet()) {
          RssProtos.ReplicaServersItem item =
              RssProtos.ReplicaServersItem.newBuilder()
                  .addAllServerId(ShuffleServerInfo.toProto(replicaServerEntry.getValue()))
                  .build();
          replicaServersProto.put(replicaServerEntry.getKey(), item);
        }

        RssProtos.PartitionReplicaServers partitionReplicaServerProto =
            RssProtos.PartitionReplicaServers.newBuilder()
                .putAllReplicaServers(replicaServersProto)
                .build();
        partitionToServers.put(partitionId, partitionReplicaServerProto);
      }

      RssProtos.ShuffleHandleInfo handleProto =
          RssProtos.ShuffleHandleInfo.newBuilder()
              .setShuffleId(handleInfo.shuffleId)
              .setRemoteStorageInfo(
                  RssProtos.RemoteStorageInfo.newBuilder()
                      .setPath(handleInfo.remoteStorage.getPath())
                      .putAllConfItems(handleInfo.remoteStorage.getConfItems())
                      .build())
              .putAllPartitionToServers(partitionToServers)
              .build();
      return handleProto;
    }
  }

  public static ShuffleHandleInfo fromProto(RssProtos.ShuffleHandleInfo handleProto) {
    if (handleProto == null) {
      return null;
    }
    Map<Integer, Map<Integer, List<ShuffleServerInfo>>> partitionToServers = new HashMap<>();
    for (Map.Entry<Integer, RssProtos.PartitionReplicaServers> entry :
        handleProto.getPartitionToServersMap().entrySet()) {
      Map<Integer, List<ShuffleServerInfo>> replicaServers =
          partitionToServers.computeIfAbsent(entry.getKey(), x -> new HashMap<>());
      for (Map.Entry<Integer, RssProtos.ReplicaServersItem> serverEntry :
          entry.getValue().getReplicaServersMap().entrySet()) {
        int replicaIdx = serverEntry.getKey();
        List<ShuffleServerInfo> shuffleServerInfos =
            ShuffleServerInfo.fromProto(serverEntry.getValue().getServerIdList());
        replicaServers.put(replicaIdx, shuffleServerInfos);
      }
    }
    RemoteStorageInfo remoteStorageInfo =
        new RemoteStorageInfo(
            handleProto.getRemoteStorageInfo().getPath(),
            handleProto.getRemoteStorageInfo().getConfItemsMap());
    ShuffleHandleInfo handle = new ShuffleHandleInfo();
    handle.shuffleId = handle.getShuffleId();
    handle.partitionReplicaAssignedServers = partitionToServers;
    handle.remoteStorage = remoteStorageInfo;
    return handle;
  }

  public Set<String> listExcludedServers() {
    return excludedServerToReplacements.keySet();
  }

  public void checkPartitionReassignServerNum(
      Set<Integer> partitionIds, int legalReassignServerNum) {
    for (int partitionId : partitionIds) {
      Map<Integer, List<ShuffleServerInfo>> replicas =
          partitionReplicaAssignedServers.get(partitionId);
      for (List<ShuffleServerInfo> servers : replicas.values()) {
        if (servers.size() - 1 > legalReassignServerNum) {
          throw new RssException(
              "Illegal reassignment servers for partitionId: "
                  + partitionId
                  + " that exceeding the max legal reassign server num: "
                  + legalReassignServerNum);
        }
      }
    }
  }
}
