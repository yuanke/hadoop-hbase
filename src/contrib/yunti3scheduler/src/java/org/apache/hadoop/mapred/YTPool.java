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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.mapred.Yunti3Scheduler.JobInfo;

/**
 * A schedulable pool of jobs.
 */
public class YTPool {
  /** Name of the default pool, where jobs with no pool parameter go. */
  public static final String DEFAULT_POOL_NAME = "default";
  
  /** Pool name. */
  private String name;
  
  /** Map from jobs in this specific pool to per-job scheduling variables */
  private Map<JobInProgress, JobInfo> jobInfos = new HashMap<JobInProgress, JobInfo>();
      
  // current runnable jobs, order by jobLevel then time
  private List<JobInProgress> runnableJobs = new ArrayList<JobInProgress>();

  // a comparator to sort jobs based on order: level, start time, jobID 
  final Comparator<JobInProgress > LEVEL_BASED_FIFO_JOB_COMPARATOR
    = new Comparator<JobInProgress >() {
    public int compare(JobInProgress  j1, JobInProgress  j2) {
      int res = getJobLevel(j2) - getJobLevel(j1);
      if (res == 0) {
        if (j1.getStartTime() < j2.getStartTime()) {
          res = -1;
        } else if (j1.getStartTime() > j2.getStartTime()){
          res = 1;
        }
      }

      if (res == 0) {
        res = j1.getJobID().compareTo(j2.getJobID());
      }
      return res;
    }
  };
  
  //order by jobLevel then time 
  private Set<JobInProgress> orderedJobs = new TreeSet<JobInProgress>(LEVEL_BASED_FIFO_JOB_COMPARATOR);
  
  // total running maps in this pool
  private int totalRunningMaps;
  // total running reduces in this pool
  private int totalRunningReduces;
  
  public YTPool(String name) {
    this.name = name;
  }
  
  public Collection<JobInProgress> getJobs() {
    return jobInfos.keySet();
  }
  
  public Map<JobInProgress, JobInfo> getJobInfos() {
    return jobInfos;
  }
  
  public JobInfo getJobInfo(JobInProgress job) {
    return jobInfos.get(job);
  }
  
  public void addJob(JobInProgress job) {
    JobInfo info = new JobInfo();
    addJob(job, info);
  }
  
  public void addJob(JobInProgress job, JobInfo info) {
    info.jobLevel = getJobLevel(job);
    jobInfos.put(job, info);
    orderedJobs.add(job);
  }
 
 
  /**
   * Get the ordered candidate jobs of this pool to be scheduled
   * @return jobs to be scheduled
   */
  public Set<JobInProgress> getOrderedJobs() {            
    return orderedJobs;
  }
  
  /**
   * Get the runnable jobs of this pool to be scheduled   
   * @return jobs to be scheduled
   */
  public List<JobInProgress> getRunnableJobs() {    
    return runnableJobs;
  }
  
   
 
  public void removeJob(JobInProgress job) {
    jobInfos.remove(job);
    
    orderedJobs.remove(job);
  }
  
  public String getName() {
    return name;
  }

  // Getter methods for reading JobInfo values based on TaskType, safely
  // returning 0's for jobs with no JobInfo present.

  public int neededTasks(JobInProgress job, YTTaskType taskType) {
    JobInfo info = jobInfos.get(job);
    if (info == null) return 0;
    return taskType == YTTaskType.MAP ? info.neededMaps : info.neededReduces;
  }
  
  public int runningTasks(JobInProgress job, YTTaskType taskType) {
    JobInfo info = jobInfos.get(job);
    if (info == null) return 0;
    return taskType == YTTaskType.MAP ? info.runningMaps : info.runningReduces;
  }

  public int runnableTasks(JobInProgress job, YTTaskType type) {
    return neededTasks(job, type) + runningTasks(job, type);
  }

  public int minTasks(JobInProgress job, YTTaskType type) {
    JobInfo info = jobInfos.get(job);
    if (info == null) return 0;
    return (type == YTTaskType.MAP) ? (int)info.mapFairShare : (int)info.reduceFairShare;
  }

  public double weight(JobInProgress job, YTTaskType taskType) {
    JobInfo info = jobInfos.get(job);
    if (info == null) return 0;
    return (taskType == YTTaskType.MAP ? info.mapWeight : info.reduceWeight);
  }
  
  public boolean isRunnable(JobInProgress job) {
    JobInfo info = jobInfos.get(job);
    if (info == null) return false;
    return info.runnable;
  }
  
  public int getJobLevel(JobInProgress job) {
    JobInfo info = jobInfos.get(job);
    if (info == null) {
      int level = job.getJobConf().getInt("mapred.job.level", 0);
      return (level < 0) ? 0 : level;
    } else {
      return info.jobLevel;
    }
  }
  
  /**
   * Update job's level dynamically. Now this API is called from 
   * scheduler's ui to change job level. If level is not valid number
   * format or this pool does't have the job, the operation will be 
   * ignored.
   * @param jobId 
   * @param incr a String containing level increment eg."+1" 
   */
  public void updateJobLevel(String jobId, String incr) {
    incr = incr.trim();
    if (incr.startsWith("+")) {
      incr = incr.substring(1); // remove plus sign
    }
    
    int iIncr = 0;
    try {
      iIncr = Integer.parseInt(incr);
    } catch (NumberFormatException e) {
      e.printStackTrace();
      return;
    }

    for (JobInProgress job : jobInfos.keySet()) {
      if (job.getJobID().toString().equals(jobId)) {
        JobInfo info = jobInfos.get(job);
        setJobLevel(job, info.jobLevel + iIncr);
        break;
      }
    }
  }
  
  /**
   * Update job's level dynamically. If this pool does not 
   * contain the job the operation will be ignored. If level
   * is negative, it will be reset to zero.  
   * @param job
   * @param level
   */
  public void setJobLevel(JobInProgress job, int level) {
    // job level shout be nonnegative number
    level = (level < 0) ? 0 : level;
    
    JobInfo info = jobInfos.get(job);
    if (info != null) {
      info.jobLevel = level;
      
      //remove the job and add it to orderdJobs by the order of job level 
      orderedJobs.remove(job);
      addJob(job, info);
    }
  }
  
  public int getTotalRunningMaps() {
    return totalRunningMaps;
  }

  public void setTotalRunningMaps(int totalRunningMaps) {
    this.totalRunningMaps = totalRunningMaps;
  }

  public int getTotalRunningReduces() {
    return totalRunningReduces;
  }

  public void setTotalRunningReduces(int totalRunningReduces) {
    this.totalRunningReduces = totalRunningReduces;
  }

  public void setRunnableJobs(List<JobInProgress> newRunnableJobs) {    
    runnableJobs = newRunnableJobs;  
  }
}
