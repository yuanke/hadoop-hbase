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
import java.util.HashMap;
import java.util.Map;

public class MapOutputWatcher {

  private final JobConf conf;
  private final int maxShufflingPerDisk;
  private final int maxShufflingPerJob;
  private final long maxMapOutputSizeSkip;
  
  /**
   * A map of local directory -> no. of work threads shuffling from this disk concurrently
   */
  private final Map<String, Integer> localDirToShufflingNo = new HashMap<String, Integer>();
  
  /**
   * A map of jobId(String) -> no. of work threads occupied concurrently
   */
  private final Map<String, Integer> jobIdToShufflingNo = new HashMap<String, Integer>();
  
  public MapOutputWatcher(JobConf conf) throws IOException{
    this.conf = conf;
    int workerThreads = this.conf.getInt("tasktracker.http.threads", 40);
    float maxShufflePercentPerJob = this.conf.getFloat("mapred.job.shuffle.max.percent", 0.5f);
    maxShufflingPerJob = (int) (workerThreads * maxShufflePercentPerJob + 0.5f);
    
    // get local directories
    String[] localDirs = this.conf.getLocalDirs();
    // 16 for twelve disks, 32 for six disks when 100 worker threads.
    maxShufflingPerDisk = (workerThreads / localDirs.length) * 2; 
  
    // allow shuffle a few data
    maxMapOutputSizeSkip = this.conf.getLong("mapred.mapoutput.max.size.skip", 64 * 1024);
  }
  
  /**
   * Can this job shuffle this map's output now? Whatever, we still increment the counter.
   * 
   * @param localDir local directory of the map output file, e.g /disk0/mapred/local/
   * @param jobId job id with string, e.g job_201011031423_0002
   * @param rawLength data size of this shuffle
   * @return true if can shuffle now 
   */
  public synchronized boolean canShuffleAndInr(String localDir, String jobId, long rawLength) {
    boolean canShuffle = canAndIncr(localDir, maxShufflingPerDisk, localDirToShufflingNo)
                      && canAndIncr(jobId, maxShufflingPerJob, jobIdToShufflingNo);
    return canShuffle || rawLength <= maxMapOutputSizeSkip;
  }
  
  private static boolean canAndIncr(String key, int maxValue, Map<String, Integer> counter) {
    Integer value = counter.get(key);
    if (value == null) {
      value = 0;
    }
    
    boolean canIncr = value < maxValue;  
    counter.put(key, value + 1);
    return canIncr;
  }
  
  private static void decr(String key, Map<String, Integer> counter) {
    Integer value = counter.get(key);
    if (value == null)  {
      return;
    }
    
    value--;
    if (value <= 0) {
      counter.remove(key);
    } else {
      counter.put(key, value);
    }
  }
  /**
   * When a shuffle finished or failed, we just decrease the according count.
   * @param localDir local directory of the map output file, e.g /disk0/mapred/local/
   * @param jobId job id with string, e.g job_201011031423_0002
   */
  public synchronized void shuffleDecr(String localDir, String jobId) {
    decr(localDir, localDirToShufflingNo);
    decr(jobId, jobIdToShufflingNo);
  }
  
  public static class ShuffleRefusedException extends IOException {

    private static final long serialVersionUID = 1L;
    
    public ShuffleRefusedException(String msg) {
      super(msg);
    }
  }
}
