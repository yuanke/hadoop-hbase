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

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * A pluggable object that manages the load on each {@link TaskTracker}, telling
 * the {@link TaskScheduler} when it can launch new tasks. 
 */
public abstract class YTLoadManager implements Configurable {
  protected Configuration conf;
  protected TaskTrackerManager taskTrackerManager;
  
  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public synchronized void setTaskTrackerManager(
      TaskTrackerManager taskTrackerManager) {
    this.taskTrackerManager = taskTrackerManager;
  }
  
  /**
   * Lifecycle method to allow the LoadManager to start any work in separate
   * threads.
   */
  public void start() throws IOException {
    // do nothing
  }
  
  /**
   * Lifecycle method to allow the LoadManager to stop any work it is doing.
   */
  public void terminate() throws IOException {
    // do nothing
  }
  
  /**
   * How many map task can run on the given {@link TaskTracker}?
   * @param tracker The machine we wish to run a new map on
   * @param totalRunnableMaps Set of running jobs in the cluster
   * @param totalMapSlots The total number of map slots in the cluster
   * @param mapCapacity Maximum number of map slots can assign 
   * @return number of map task can run on <code>tracker</code>
   */
  public abstract int canAssignMapNum(TaskTrackerStatus tracker,
      int totalRunnableMaps, int totalMapSlots, int mapCapacity);

  /**
   * How many reduce task can run on the given {@link TaskTracker}?
   * @param tracker The machine we wish to run a new map on
   * @param totalRunnableReduces Set of running jobs in the cluster
   * @param totalReduceSlots The total number of reduce slots in the cluster
   * @param reduceCapacity Maximum number of reduce slots can assign 
   * @return number of map task can run on <code>tracker</code>
   */
  public abstract int canAssignReduceNum(TaskTrackerStatus tracker,
      int totalRunnableReduces, int totalReduceSlots, int reduceCapacity);
}
