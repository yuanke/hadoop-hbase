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

import java.util.Comparator;

import org.apache.hadoop.mapred.Yunti3Scheduler.JobInfo;

/**
 * Order {@link JobInProgress} objects by priority and then by submit time, as
 * in the default scheduler in Hadoop.
 */
public class YTFifoJobComparator implements Comparator<JobInProgress> {
  private final YTPool curPool;
  public YTFifoJobComparator(YTPool pool) {
    this.curPool = pool;
  }
  public int compare(JobInProgress j1, JobInProgress j2) {
    JobInfo info1 = curPool.getJobInfo(j1);
    JobInfo info2 = curPool.getJobInfo(j2);
    
    if (info1.jobLevel > info2.jobLevel) {
      return -1;
    }
    else if (info1.jobLevel < info2.jobLevel) {
      return 1;
    }
    int res = j1.getPriority().compareTo(j2.getPriority());
    if (res == 0) {
      if (j1.getStartTime() < j2.getStartTime()) {
        res = -1;
      } else {
        res = (j1.getStartTime() == j2.getStartTime()
            ? j1.getJobID().compareTo(j2.getJobID()) : 1);
      }
    }
    return res;
  }
}
