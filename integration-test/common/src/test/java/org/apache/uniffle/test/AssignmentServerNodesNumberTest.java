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

package org.apache.uniffle.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import org.apache.uniffle.client.impl.ShuffleWriteClientImpl;
import org.apache.uniffle.client.util.ClientType;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.config.RssBaseConf;
import org.apache.uniffle.coordinator.CoordinatorConf;
import org.apache.uniffle.server.ShuffleServerConf;
import org.apache.uniffle.storage.util.StorageType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssignmentServerNodesNumberTest extends CoordinatorTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(AssignmentServerNodesNumberTest.class);
  private static final int SHUFFLE_NODES_MAX = 10;
  private static final int SERVER_NUM = 10;
  private static final HashSet<String> TAGS = Sets.newHashSet("t1");

  @BeforeAll
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.setLong(CoordinatorConf.COORDINATOR_APP_EXPIRED, 2000);
    coordinatorConf.setInteger(CoordinatorConf.COORDINATOR_SHUFFLE_NODES_MAX, SHUFFLE_NODES_MAX);
    createCoordinatorServer(coordinatorConf);

    for (int i = 0; i < SERVER_NUM; i++){
      ShuffleServerConf shuffleServerConf = getShuffleServerConf();
      File tmpDir = Files.createTempDir();
      File dataDir1 = new File(tmpDir, "data1");
      String basePath = dataDir1.getAbsolutePath();
      shuffleServerConf.set(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.MEMORY_LOCALFILE_HDFS.name());
      shuffleServerConf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, basePath);
      shuffleServerConf.set(RssBaseConf.RPC_METRICS_ENABLED, true);
      shuffleServerConf.set(ShuffleServerConf.SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT, 2000L);
      shuffleServerConf.set(ShuffleServerConf.SERVER_PRE_ALLOCATION_EXPIRED, 5000L);
      shuffleServerConf.setInteger(RssBaseConf.RPC_SERVER_PORT, 18001 + i);
      shuffleServerConf.setInteger(RssBaseConf.JETTY_HTTP_PORT, 19010 + i);
      shuffleServerConf.set(ShuffleServerConf.TAGS, new ArrayList<>(TAGS));
      createShuffleServer(shuffleServerConf);
    }
    startServers();

    Thread.sleep(1000 * 5);
  }

  @Test
  public void testAssignmentServerNodesNumber() throws Exception {
    ShuffleWriteClientImpl shuffleWriteClient = new ShuffleWriteClientImpl(ClientType.GRPC.name(), 3, 1000, 1,
        1, 1, 1, true, 1, 1);
    shuffleWriteClient.registerCoordinators(COORDINATOR_QUORUM);

    /**
     * case1: user specify the illegal shuffle node num(<0)
     * it will use the default shuffle nodes num when having enough servers.
     */
    ShuffleAssignmentsInfo info = shuffleWriteClient.getShuffleAssignments("app1", 0, 10, 1, TAGS, -1);
    assertEquals(SHUFFLE_NODES_MAX, info.getServerToPartitionRanges().keySet().size());

    /**
     * case2: user specify the illegal shuffle node num(>default max limitation)
     * it will use the default shuffle nodes num when having enough servers
     */
    info = shuffleWriteClient.getShuffleAssignments("app1", 0, 10, 1, TAGS, SERVER_NUM + 10);
    assertEquals(SHUFFLE_NODES_MAX, info.getServerToPartitionRanges().keySet().size());

    /**
     * case3: user specify the legal shuffle node num,
     * it will use the customized shuffle nodes num when having enough servers
     */
    info = shuffleWriteClient.getShuffleAssignments("app1", 0, 10, 1, TAGS, SERVER_NUM - 1);
    assertEquals(SHUFFLE_NODES_MAX - 1, info.getServerToPartitionRanges().keySet().size());
  }
}
