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

package org.apache.hadoop.mapred;

/**
 * A {@link YTLoadManager} for use by the {@link Yunti3Scheduler} that allocates
 * tasks evenly across nodes up to their per-node maximum, using the default
 * load management algorithm in Hadoop.
 */
public class YTCapBasedLoadManager extends YTLoadManager {
  /**
   * Determine how many tasks of a given type we want to run on a TaskTracker. 
   * This cap is chosen based on how many tasks of that type are outstanding in
   * total, so that when the cluster is used below capacity, tasks are spread
   * out uniformly across the nodes rather than being clumped up on whichever
   * machines sent out heartbeats earliest.
   */

  @Override
  public int canAssignMapNum(TaskTrackerStatus tracker, int totalRunnableMaps,
      int totalMapSlots, int mapCapacity) {
    double load = ((double) totalRunnableMaps) / totalMapSlots;    
    int newCap = (int) Math.ceil(tracker.getMaxMapSlots() * load) - tracker.countMapTasks();
    return Math.min(newCap, mapCapacity);
  }

  @Override
  public int canAssignReduceNum(TaskTrackerStatus tracker,
      int totalRunnableReduces, int totalReduceSlots, int reduceCapacity) { 
    double load = ((double)totalRunnableReduces) / totalReduceSlots;   
    int newCap = (int) Math.ceil(tracker.getMaxReduceSlots() * load) - tracker.countReduceTasks();
    return Math.min(newCap, reduceCapacity);
  }
}
