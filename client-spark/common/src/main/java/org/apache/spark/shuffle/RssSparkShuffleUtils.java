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

import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.CoordinatorClient;
import org.apache.uniffle.client.factory.CoordinatorClientFactory;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.common.util.Constants;

import static org.apache.spark.shuffle.RssSparkClientConf.SPARK_CONFIG_KEY_PREFIX;

public class RssSparkShuffleUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RssSparkShuffleUtils.class);

  public static Configuration newHadoopConfiguration(SparkConf sparkConf) {
    SparkHadoopUtil util = new SparkHadoopUtil();
    Configuration conf = util.newConfiguration(sparkConf);

    boolean useOdfs = sparkConf.getBoolean(
        SPARK_CONFIG_KEY_PREFIX + RssSparkClientConf.RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE.key(),
        false
    );
    if (useOdfs) {
      final int OZONE_PREFIX_LEN = "spark.rss.ozone.".length();
      conf.setBoolean(
          SPARK_CONFIG_KEY_PREFIX
              + RssSparkClientConf.RSS_OZONE_DFS_NAMENODE_ODFS_ENABLE.key().substring(OZONE_PREFIX_LEN),
          useOdfs
      );
      conf.set(
          SPARK_CONFIG_KEY_PREFIX
              + RssSparkClientConf.RSS_OZONE_FS_HDFS_IMPL.key().substring(OZONE_PREFIX_LEN),
          sparkConf.get(
              SPARK_CONFIG_KEY_PREFIX + RssSparkClientConf.RSS_OZONE_FS_HDFS_IMPL,
              RssSparkClientConf.RSS_OZONE_FS_HDFS_IMPL.defaultValue())
      );
      conf.set(
          SPARK_CONFIG_KEY_PREFIX
              + RssSparkClientConf.RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL.key().substring(OZONE_PREFIX_LEN),
          sparkConf.get(SPARK_CONFIG_KEY_PREFIX + RssSparkClientConf.RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL,
              RssSparkClientConf.RSS_OZONE_FS_ABSTRACT_FILE_SYSTEM_HDFS_IMPL.defaultValue()));
    }

    return conf;
  }

  public static ShuffleManager loadShuffleManager(String name, SparkConf conf, boolean isDriver) throws Exception {
    Class<?> klass = Class.forName(name);
    Constructor<?> constructor;
    ShuffleManager instance;
    try {
      constructor = klass.getConstructor(conf.getClass(), Boolean.TYPE);
      instance = (ShuffleManager) constructor.newInstance(conf, isDriver);
    } catch (NoSuchMethodException e) {
      constructor = klass.getConstructor(conf.getClass());
      instance = (ShuffleManager) constructor.newInstance(conf);
    }
    return instance;
  }

  public static List<CoordinatorClient> createCoordinatorClients(SparkConf sparkConf) throws RuntimeException {
    RssSparkClientConf clientConf = RssSparkClientConf.from(sparkConf);
    String clientType = clientConf.get(RssSparkClientConf.RSS_CLIENT_TYPE);
    String coordinators = clientConf.get(RssSparkClientConf.RSS_COORDINATOR_QUORUM);
    CoordinatorClientFactory coordinatorClientFactory = new CoordinatorClientFactory(clientType);
    return coordinatorClientFactory.createCoordinatorClient(coordinators);
  }

  public static void validateRssClientConf(RssConf rssConf) {
    String msgFormat = "%s must be set by the client or fetched from coordinators.";
    if (!rssConf.contains(RssSparkClientConf.RSS_STORAGE_TYPE)) {
      String msg = String.format(msgFormat, "Storage type");
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
  }

  public static Configuration getRemoteStorageHadoopConf(
      SparkConf sparkConf, RemoteStorageInfo remoteStorageInfo) {
    Configuration readerHadoopConf = RssSparkShuffleUtils.newHadoopConfiguration(sparkConf);
    final Map<String, String> shuffleRemoteStorageConf = remoteStorageInfo.getConfItems();
    if (shuffleRemoteStorageConf != null && !shuffleRemoteStorageConf.isEmpty()) {
      for (Map.Entry<String, String> entry : shuffleRemoteStorageConf.entrySet()) {
        readerHadoopConf.set(entry.getKey(), entry.getValue());
      }
    }
    return readerHadoopConf;
  }

  public static Set<String> getAssignmentTags(RssConf rssConf) {
    Set<String> assignmentTags = new HashSet<>();
    List<String> tags = rssConf.get(RssSparkClientConf.RSS_CLIENT_ASSIGNMENT_TAGS);
    if (CollectionUtils.isNotEmpty(tags)) {
      assignmentTags.addAll(tags);
    }
    assignmentTags.add(Constants.SHUFFLE_SERVER_VERSION);
    return assignmentTags;
  }
}
