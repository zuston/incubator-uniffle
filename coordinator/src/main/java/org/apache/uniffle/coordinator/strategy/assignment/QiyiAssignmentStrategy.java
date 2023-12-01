package org.apache.uniffle.coordinator.strategy.assignment;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.coordinator.ClusterManager;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.coordinator.ServerNode;
import org.apache.uniffle.coordinator.util.CoordinatorUtils;

/**
 * 用于当集群中存在不同内存容量的机型时，避免高内存机型磁盘满的情况
 */
public class QiyiAssignmentStrategy extends AbstractAssignmentStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(QiyiAssignmentStrategy.class);

  private ClusterManager clusterManager;

  private Map<ServerNode, PartitionBalanceAssignmentStrategy.PartitionAssignmentInfo> serverToPartitions;

  private RangeMap<Long, Double> weightRangeMap;

  public QiyiAssignmentStrategy(ClusterManager clusterManager, CoordinatorConf conf) {
    super(conf);
    this.clusterManager = clusterManager;
    this.serverToPartitions = new ConcurrentHashMap<>();

    TreeRangeMap weightRangeMap = TreeRangeMap.create();
    String weightConf = conf.get(CoordinatorConf.COORDINATOR_QIYI_ASSIGNMENT_MEMORY_WEIGHT_CONF);
    if (!StringUtils.isEmpty(weightConf)) {
      // 当前内存用量区间:降低权重的比例 (单位是 G)
      // 140-200:0.2
      // 上面这个意味着对当前内存使用量为 140-200 之间的 server. *0.2 的权重要素

      for (String grp : weightConf.split(",")) {
        String[] arr = grp.split(":", 2);
        String left = arr[0];
        double ratio = Double.parseDouble(arr[1]);
        arr = left.split("-", 2);
        weightRangeMap.put(
            Range.closedOpen(
                Long.parseLong(arr[0]) * 1024 * 1024 * 1024,
                Long.parseLong(arr[1]) * 1024 * 1024 * 1024
            ),
            ratio
        );
      }
    }
    this.weightRangeMap = weightRangeMap;
  }

  private PartitionRangeAssignment assign(
      int totalPartitionNum,
      int partitionNumPerRange,
      int replica,
      Set<String> requiredTags,
      int requiredShuffleServerNumber,
      Set<String> faultyServerIds) {

    if (partitionNumPerRange != 1) {
      throw new RssException("PartitionNumPerRange must be one");
    }

    SortedMap<PartitionRange, List<ServerNode>> assignments = new TreeMap<>();
    synchronized (this) {
      List<ServerNode> nodes = clusterManager.getServerList(requiredTags);
      nodes = nodes.stream().filter(x -> !faultyServerIds.contains(x)).collect(Collectors.toList());

      Map<ServerNode, PartitionBalanceAssignmentStrategy.PartitionAssignmentInfo> newPartitionInfos =
          Maps.newConcurrentMap();
      for (ServerNode node : nodes) {
        PartitionBalanceAssignmentStrategy.PartitionAssignmentInfo partitionInfo;
        if (serverToPartitions.containsKey(node)) {
          partitionInfo = serverToPartitions.get(node);
          if (partitionInfo.getTimestamp() < node.getTimestamp()) {
            partitionInfo.resetPartitionNum();
            partitionInfo.setTimestamp(node.getTimestamp());
          }
        } else {
          partitionInfo = new PartitionBalanceAssignmentStrategy.PartitionAssignmentInfo();
        }
        newPartitionInfos.putIfAbsent(node, partitionInfo);
      }
      serverToPartitions = newPartitionInfos;
      int averagePartitions = totalPartitionNum * replica / clusterManager.getShuffleNodesMax();
      int assignPartitions = averagePartitions < 1 ? 1 : averagePartitions;
      nodes.sort(new Comparator<ServerNode>() {
        @Override
        public int compare(ServerNode o1, ServerNode o2) {
          PartitionBalanceAssignmentStrategy.PartitionAssignmentInfo partitionInfo1 = serverToPartitions.get(o1);
          PartitionBalanceAssignmentStrategy.PartitionAssignmentInfo partitionInfo2 = serverToPartitions.get(o2);

          long o1AvailableMem = o1.getAvailableMemory();
          long o2AvailableMem = o2.getAvailableMemory();

          Double o1WeightRatio = weightRangeMap.get(o1AvailableMem);
          if (o1WeightRatio != null) {
            o1AvailableMem *= o1WeightRatio;
          }
          Double o2WeightRatio = weightRangeMap.get(o2AvailableMem);
          if (o2WeightRatio != null) {
            o2AvailableMem *= o2WeightRatio;
          }

          double v1 = o1AvailableMem * 1.0 / (partitionInfo1.getPartitionNum() + assignPartitions);
          double v2 = o2AvailableMem * 1.0 / (partitionInfo2.getPartitionNum() + assignPartitions);
          return -Double.compare(v1, v2);
        }
      });

      if (nodes.isEmpty() || nodes.size() < replica) {
        throw new RssException("There isn't enough shuffle servers");
      }

      final int assignmentMaxNum = clusterManager.getShuffleNodesMax();
      int expectNum = assignmentMaxNum;
      if (requiredShuffleServerNumber < assignmentMaxNum && requiredShuffleServerNumber > 0) {
        expectNum = requiredShuffleServerNumber;
      }

      if (nodes.size() < expectNum) {
        LOG.warn("Can't get expected servers [" + expectNum + "] and found only [" + nodes.size() + "]");
        expectNum = nodes.size();
      }

      List<ServerNode> candidatesNodes = nodes.subList(0, expectNum);
      int idx = 0;
      List<PartitionRange> ranges = CoordinatorUtils.generateRanges(totalPartitionNum, 1);
      for (PartitionRange range : ranges) {
        List<ServerNode> assignNodes = Lists.newArrayList();
        for (int rc = 0; rc < replica; rc++) {
          ServerNode node =  candidatesNodes.get(idx);
          idx = CoordinatorUtils.nextIdx(idx,  candidatesNodes.size());
          serverToPartitions.get(node).incrementPartitionNum();
          assignNodes.add(node);
        }
        assignments.put(range, assignNodes);
      }
    }
    return new PartitionRangeAssignment(assignments);
  }

  @Override
  public PartitionRangeAssignment assign(int totalPartitionNum, int partitionNumPerRange, int replica,
      Set<String> requiredTags, int requiredShuffleServerNumber, int estimateTaskConcurrency) {
    return assign(totalPartitionNum, partitionNumPerRange, replica, requiredTags,
        requiredShuffleServerNumber, estimateTaskConcurrency, new HashSet<>());
  }

  @Override
  public PartitionRangeAssignment assign(int totalPartitionNum, int partitionNumPerRange, int replica,
      Set<String> requiredTags, int requiredShuffleServerNumber, int estimateTaskConcurrency,
      Set<String> faultyServerIds) {
    return assign(totalPartitionNum, partitionNumPerRange, replica, requiredTags,
        requiredShuffleServerNumber, faultyServerIds);
  }
}
