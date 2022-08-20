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

package org.apache.uniffle.storage;

import java.io.File;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HdfsTestBase implements Serializable {

  public static Configuration conf;
  protected static String HDFS_URI;
  protected static FileSystem fs;
  protected static MiniDFSCluster cluster;
  protected static File baseDir;

  private static volatile MiniHdfsCluster miniHdfsCluster;

  @BeforeAll
  public static void setUpHdfs(@TempDir File tempDir) throws Exception {
    if (conf == null) {
      synchronized (HdfsTestBase.class) {
        if (conf == null) {
          MiniHdfsCluster.setUpHdfs(tempDir);
          conf = MiniHdfsCluster.conf;
          baseDir = MiniHdfsCluster.baseDir;
          cluster = MiniHdfsCluster.cluster;
          HDFS_URI = MiniHdfsCluster.HDFS_URI;
          fs = MiniHdfsCluster.fs;
        }
      }
    }
  }

  public static void tearDownHdfs() throws Exception {
    fs.close();
    MiniHdfsCluster.cluster.shutdown();
  }

  protected void compareBytes(List<byte[]> expected, List<ByteBuffer> actual) {
    assertEquals(expected.size(), actual.size());

    for (int i = 0; i < expected.size(); i++) {
      byte[] expectedI = expected.get(i);
      ByteBuffer bb = actual.get(i);
      for (int j = 0; j < expectedI.length; j++) {
        assertEquals(expectedI[j], bb.get(j));
      }
    }
  }
}
