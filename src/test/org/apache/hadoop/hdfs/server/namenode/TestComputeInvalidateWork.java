/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;

import junit.framework.TestCase;

/**
 * Test if FSNamesystem handles heartbeat right
 */
public class TestComputeInvalidateWork extends TestCase {
  /**
   * Test if {@link FSNamesystem#computeInvalidateWork(int)} can schedule
   * invalidate work correctly
   */
  public void testCompInvalidate() throws Exception {
    final Configuration conf = new Configuration();
    final int NUM_OF_DATANODES = 3;
    final MiniDFSCluster cluster = new MiniDFSCluster(conf, NUM_OF_DATANODES,
        true, null);
    try {
      cluster.waitActive();
      final FSNamesystem namesystem = FSNamesystem.getFSNamesystem();
      DatanodeDescriptor[] nodes = namesystem.heartbeats
          .toArray(new DatanodeDescriptor[NUM_OF_DATANODES]);
      assertEquals(nodes.length, NUM_OF_DATANODES);

      long heartbeatInterval = conf.getLong("dfs.heartbeat.interval", 3) * 1000;
      int blockInvalidateLimit = Math.max(FSConstants.BLOCK_INVALIDATE_CHUNK,
          20 * (int) (heartbeatInterval / 1000));

      synchronized (namesystem) {
        for (int i = 0; i < nodes.length; i++) {
          for (int j = 0; j < 3 * blockInvalidateLimit + 1; j++) {
            Block block = new Block(i * (blockInvalidateLimit + 1) + j, 0,
                GenerationStamp.FIRST_VALID_STAMP);
            namesystem.addToInvalidatesNoLog(block, nodes[i]);
          }
        }

        assertEquals(blockInvalidateLimit * NUM_OF_DATANODES,
            namesystem.computeInvalidateWork(NUM_OF_DATANODES + 1));
        assertEquals(blockInvalidateLimit * NUM_OF_DATANODES,
            namesystem.computeInvalidateWork(NUM_OF_DATANODES));
        assertEquals(blockInvalidateLimit * (NUM_OF_DATANODES - 1),
            namesystem.computeInvalidateWork(NUM_OF_DATANODES - 1));
        int workCount = namesystem.computeInvalidateWork(1);
        if (workCount == 1) {
          assertEquals(blockInvalidateLimit + 1,
              namesystem.computeInvalidateWork(2));
        } else {
          assertEquals(workCount, blockInvalidateLimit);
          assertEquals(2, namesystem.computeInvalidateWork(2));
        }
      }
    } finally {
      cluster.shutdown();
    }
  }
}
