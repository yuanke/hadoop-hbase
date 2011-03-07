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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A {@link TaskScheduler} that implements fair sharing.
 */
public class Yunti3Scheduler extends TaskScheduler {
  /** How often fair shares are re-calculated */
  public static long UPDATE_INTERVAL = 1000;
  public static final Log LOG = LogFactory.getLog(Yunti3Scheduler.class);
  
  protected YTPoolManager poolMgr;
  
  protected YTLoadManager loadMgr;
  protected YTTaskSelector taskSelector;
  protected YTWeightAdjuster weightAdjuster; // Can be null for no weight adjuster
  protected boolean initialized;  // Are we initialized?
  protected boolean running;      // Are we running?
  protected boolean assignMultiple; // Simultaneously assign map and reduce?
  protected int mapAssignCap = -1;    // Max maps to launch per heartbeat, -1 to use
                                     // the TaskTracker's slot count
  protected int reduceAssignCap = -1; // Max reduces to launch per heartbeat, 
                                     // -1 to use the TaskTracker's slot count
  protected long localityDelay = MAX_AUTOCOMPUTED_LOCALITY_DELAY; 
                                     // Time to wait for node and rack locality
  protected boolean autoComputeLocalityDelay = false; // Compute locality delay
  // Maximum locality delay when auto-computing locality delays
  private static final long MAX_AUTOCOMPUTED_LOCALITY_DELAY = 15000;

  // from heartbeat interval
  protected boolean sizeBasedWeight; // Give larger weights to larger jobs
  protected boolean waitForMapsBeforeLaunchingReduces = true;
  private Clock clock;
  private int tickCount;
  private EagerTaskInitializationListener eagerInitListener;
  private JobListener jobListener; 
  private boolean mockMode;     // Used for unit tests; disables background updates
                                // and scheduler event log
  
  private int runnableMaps = 0;
  private int runnableReduces = 0;
  private int totalMapSlots  = 0;
  private int totalReduceSlots = 0;
  
  protected long lastHeartbeatTime; // Time we last ran assignTasks
  
  /**
   * A class for holding per-job scheduler variables. These always contain the
   * values of the variables at the last update(), and are used along with a
   * time delta to update the map and reduce deficits before a new update().
   */
  static class JobInfo {
    boolean runnable = false;   // Can the job run given user/pool limits?    
    double mapWeight = 0;       // Weight of job in calculation of map share
    double reduceWeight = 0;    // Weight of job in calculation of reduce share
    int runningMaps = 0;        // Maps running at last update
    int runningReduces = 0;     // Reduces running at last update
    int neededMaps;             // Maps needed at last update
    int neededReduces;          // Reduces needed at last update
    double mapFairShare = 0;    // Fair share of map slots at last update
    double reduceFairShare = 0; // Fair share of reduce slots at last update
    int jobLevel = 0;           // Level of job in a queue
    LocalityLevel lastMapLocalityLevel = LocalityLevel.NODE; // Locality level of last map launched
    long timeWaitedForLocalMap = 0; // Time waiting for local map since last map
    int assignedAtLastHeartbeat = 0;
    int skippedAtLastHeartbeat = 0; // Was job skipped at previous assignTasks?

  }
  
  /**
   * A clock class - can be mocked out for testing.
   */
  static class Clock {
    long getTime() {
      return System.currentTimeMillis();
    }
  }
  
  /**
   * Top level scheduling information to be set to the queueManager
   *  added by liangly
   */
  private static class SchedulingInfo {
    private YTPool pool;
    private YTPoolManager poolMgr;
    private QueueManager queueMgr;

    public SchedulingInfo(YTPool pool, YTPoolManager poolMgr, QueueManager queueMgr) {
      this.pool = pool;
      this.poolMgr = poolMgr;
      this.queueMgr = queueMgr;
    }

    @Override
    public String toString(){
      int runningJobs = pool.getJobs().size();
      int maxJobs = poolMgr.getPoolMaxJobs(pool.getName());
      int runningMaps = pool.getTotalRunningMaps();
      int runningReduces = pool.getTotalRunningReduces();
      int minMaps = poolMgr.getAllocation(pool.getName(), YTTaskType.MAP);
      int minReduces = poolMgr.getAllocation(pool.getName(), YTTaskType.REDUCE);

      StringBuilder sb = new StringBuilder();
      sb.append(String.format("Running Jobs: %d/%d, ", runningJobs, maxJobs));
      sb.append(String.format("Running Maps: %d/%d, ", runningMaps, minMaps));
      sb.append(String.format("Running Reduces: %d/%d\n", runningReduces, minReduces));

      String info = sb.toString();
      float highlightThreshold = queueMgr.getHighlightThreshold();

      // highlight scheduling information with red color
      if ((minMaps > 0 && runningMaps * 1.f / minMaps > highlightThreshold) 
          || (minReduces > 0 && runningReduces * 1.f/ minReduces > highlightThreshold)) {
        info = "<font color = #FF0000>" + info + "</font>";
      }

      return info;
    }
  }

  public Yunti3Scheduler() {
    this(new Clock(), false);
  }
  
  /**
   * Constructor used for tests, which can change the clock and disable updates.
   */
  protected Yunti3Scheduler(Clock clock, boolean mockMode) {
    this.clock = clock;
    this.tickCount = 1;
    this.mockMode = mockMode;
    this.jobListener = new JobListener();
  }

  @Override
  public void start() {
    try {
      Configuration conf = getConf();
      if (!mockMode) {
        eagerInitListener = new EagerTaskInitializationListener();
        eagerInitListener.setTaskTrackerManager(taskTrackerManager);
        eagerInitListener.start();
        taskTrackerManager.addJobInProgressListener(eagerInitListener);
      }
      taskTrackerManager.addJobInProgressListener(jobListener);
      poolMgr = new YTPoolManager(conf);
      loadMgr = (YTLoadManager) ReflectionUtils.newInstance(
          conf.getClass("mapred.yunti3scheduler.loadmanager", 
              YTCapBasedLoadManager.class, YTLoadManager.class), conf);
      loadMgr.setTaskTrackerManager(taskTrackerManager);
      loadMgr.start();
      taskSelector = (YTTaskSelector) ReflectionUtils.newInstance(
          conf.getClass("mapred.yunti3scheduler.taskselector", 
              YTDefaultTaskSelector.class, YTTaskSelector.class), conf);
      taskSelector.setTaskTrackerManager(taskTrackerManager);
      taskSelector.start();
      Class<?> weightAdjClass = conf.getClass(
          "mapred.yunti3scheduler.weightadjuster", null);
      if (weightAdjClass != null) {
        weightAdjuster = (YTWeightAdjuster) ReflectionUtils.newInstance(
            weightAdjClass, conf);
      }
      reloadConfiguration(conf);
      initialized = true;
      running = true;
      // Start a thread to update deficits every UPDATE_INTERVAL
      if (!mockMode)
        new UpdateThread().start();
      // Register servlet with JobTracker's Jetty server
      if (taskTrackerManager instanceof JobTracker) {
        JobTracker jobTracker = (JobTracker) taskTrackerManager;
        StatusHttpServer infoServer = jobTracker.infoServer;
        infoServer.setAttribute("scheduler", this);
        infoServer.addServlet("scheduler", "/scheduler",
            Yunti3SchedulerServlet.class);
      }
      // set scheduling information to queue manager. added by liangly
      setSchedulerInfo();
    } catch (Exception e) {
      // Can't load one of the managers - crash the JobTracker now while it is
      // starting up so that the user notices.
      throw new RuntimeException("Failed to start YuntiScheduler", e);
    }
    LOG.info("Successfully configured YuntiScheduler");
  }
  
  /**
   * set scheduling information to queue manager.
   * 
   * @author liangly
   */
  public void setSchedulerInfo() {
    if (taskTrackerManager instanceof JobTracker) {
      QueueManager queueManager = taskTrackerManager.getQueueManager();
      for (String queueName : queueManager.getQueues()) {
        YTPool pool = poolMgr.getPool(queueName);
        SchedulingInfo schedulingInfo = new SchedulingInfo(pool, poolMgr, queueManager);
        queueManager.setSchedulerInfo(queueName, schedulingInfo);
      }
    }
  }

  @Override
  public void terminate() throws IOException {
    running = false;
    if (jobListener != null)
      taskTrackerManager.removeJobInProgressListener(jobListener);
    if (eagerInitListener != null)
      taskTrackerManager.removeJobInProgressListener(eagerInitListener);
  }
  
  /**
   * Used to listen for jobs added/removed by our {@link TaskTrackerManager}.
   */
  private class JobListener extends JobInProgressListener {
    @Override
    public void jobAdded(JobInProgress job) {
      synchronized (Yunti3Scheduler.this) {
        poolMgr.addJob(job);
      }
    }
    
    @Override
    public void jobRemoved(JobInProgress job) {
      synchronized (Yunti3Scheduler.this) {
        poolMgr.removeJob(job);        
      }
    }
  
    @Override
    public void jobUpdated(JobChangeEvent event) {
    }
  }

  /**
   * A thread which calls {@link Yunti3Scheduler#update()} ever
   * <code>UPDATE_INTERVAL</code> milliseconds.
   */
  private class UpdateThread extends Thread {
    private UpdateThread() {
      super("Yunti3Scheduler update thread");
    }

    public void run() {
      while (running) {
        try {
          Thread.sleep(UPDATE_INTERVAL);
          tickCount++;
          if (tickCount == 3600) {
            tickCount = 0;
          }
          update();          
        } catch (Exception e) {
          LOG.error("Failed to update fair share calculations", e);
        }
      }
    }
  }
       
  private void updateUpdateInterval() {
    UPDATE_INTERVAL = JobTracker.getJobSlotsUpdateInterval();
  }
  
  @Override
  public synchronized List<Task> assignTasks(TaskTrackerStatus tracker)
      throws IOException {
    if (!initialized) // Don't try to assign tasks if we haven't yet started up
      return null;
    
    long currentTime = clock.getTime();
    
    // FIXME: AVOID CALCULATE FOR RUNNINGS EVERYTIME
    // Compute total running maps and reduces
    int runningMaps = 0;
    int runningReduces = 0;
    for (YTPool pool : poolMgr.getPools()) {
      runningMaps += pool.getTotalRunningMaps();
      runningReduces += pool.getTotalRunningReduces();
    }

    int mapsAssigned = 0; // loop counter for map in the below while loop
    int reducesAssigned = 0; // loop counter for reduce in the below while
    int mapCapacity = maxTasksToAssign(YTTaskType.MAP, tracker);
    int reduceCapacity = maxTasksToAssign(YTTaskType.REDUCE, tracker);
    
    mapCapacity = loadMgr.canAssignMapNum(tracker, runnableMaps, totalMapSlots, mapCapacity);
    reduceCapacity = loadMgr.canAssignReduceNum(tracker, runnableReduces, totalReduceSlots, reduceCapacity);
    
    boolean mapRejected = false; // flag used for ending the loop
    boolean reduceRejected = false; // flag used for ending the loop

    if (LOG.isDebugEnabled()) {
      LOG.debug("Task capacity for tracker " + tracker.getHost() + " is(m/r):"
          + mapCapacity + "/" + reduceCapacity);
    }
		
    // Scan to see whether any job needs to run a map, then a reduce
    ArrayList<Task> tasks = new ArrayList<Task>();

    // If a pool has no task to schedule, give the chance to another pool.
    while (true) {
      // Computing the ending conditions for the loop
      // Reject a task type if one of the following condition happens
      // 1. number of assigned task reaches per heatbeat limit
      // 2. number of running tasks reaches runnable tasks
      // 3. task is rejected by the LoadManager.canAssign
      if (!mapRejected) {
        if (mapsAssigned >= mapCapacity || runningMaps >= runnableMaps) {
          mapRejected = true;
        }
      }
      if (!reduceRejected) {
        if (reducesAssigned >= reduceCapacity || runningReduces >= runnableReduces) {
          reduceRejected = true;
        }
      }
      // Exit while (true) loop if neither maps nor reduces can be assigned
      if ( (mapRejected && reduceRejected) ||
          (!assignMultiple && tasks.size() > 0) ) {
        break; // This is the only exit of the while (true) loop
      }

      // Determine which task type to assign this time
      // First try choosing a task type which is not rejected
      YTTaskType taskType;
      if (mapRejected) {
        taskType = YTTaskType.REDUCE;
      } else if (reduceRejected) {
        taskType = YTTaskType.MAP;
      } else {
        // If both types are available, choose the task type with fewer running
        // tasks on the task tracker to prevent that task type from starving
        if (tracker.countMapTasks() <= tracker.countReduceTasks()) {
          taskType = YTTaskType.MAP;
        } else {
          taskType = YTTaskType.REDUCE;
        }
      }
      // The jobs of this pool should be scheduled in turn.
      int accessedPools = 0;
      int totalPools = poolMgr.getPools().size();
      boolean foundTask = false;
      YTPool prePool = null;
      while (accessedPools < totalPools) {
        YTPool pool = poolMgr.getCurrentScheduledPool();
        if (pool == null) // no pool to schedule
          break;
        
        // pick another pool next time
        poolMgr.updateAccessedPoolIndex();
        if (pool == prePool) {// skip the same pool
          continue;
        }
        prePool = pool;
        accessedPools++;
        
        if (taskType == YTTaskType.MAP) {
          if (pool.getTotalRunningMaps() >= 
            poolMgr.getAllocation(pool.getName(), taskType)) {
            continue;
          }
        } else if (pool.getTotalRunningReduces() >= 
            poolMgr.getAllocation(pool.getName(), taskType)) {
          continue;
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("The scheduler choose Pool: " + pool.getName()
              + "'s job to assign tasks.");
        }

        // Iterate all jobs in current pool, find a job need this type of task 
        for (JobInProgress job : pool.getRunnableJobs()) {
          JobInfo info = pool.getJobInfo(job);
          if (!needLaunchTask(taskType, info))
            continue;
          
          Task task = null;
          if (taskType == YTTaskType.MAP) {
            LocalityLevel localityLevel = getAllowedLocalityLevel(job, info,
                currentTime);
            task = taskSelector.obtainNewMapTask(tracker, job, localityLevel
                .toCacheLevelCap());
          } else {
            task = taskSelector.obtainNewReduceTask(tracker, job);
          }
          if (task != null) {
            // Update the JobInfo for this job so we account for the launched
            // tasks during this update interval and don't try to launch more
            // tasks than the job needed on future heartbeats
            foundTask = true;
            if (taskType == YTTaskType.MAP) {
              info.runningMaps++;
              info.neededMaps--;
              mapsAssigned++;
              runningMaps++;
              updateLastMapLocalityLevel(job, pool.getJobInfo(job), task,
                  tracker);
              pool.setTotalRunningMaps(pool.getTotalRunningMaps() + 1);
              pool.getJobInfo(job).assignedAtLastHeartbeat++;
            } else {
              info.runningReduces++;
              info.neededReduces--;
              reducesAssigned++;
              runningReduces++;
              pool.setTotalRunningReduces(pool.getTotalRunningReduces() + 1);
            }
            tasks.add(task);
            break;
          } else {
            // Mark any jobs that were visited for map tasks but did not launch
            // a task as skipped on this heartbeat
            if (taskType == YTTaskType.MAP) {
              pool.getJobInfo(job).skippedAtLastHeartbeat++;
            }
          }
        }

        if (foundTask)
          break;
      }

      // Reject the task type if we cannot find a task
      if (!foundTask) {
        if (taskType == YTTaskType.MAP) {
          mapRejected = true;
        } else {
          reduceRejected = true;
        }
      }
    }

    if (LOG.isDebugEnabled()) {
      for (Task task : tasks) {
        LOG.debug("Assign task : " + task.getTaskID() + " to TaskTracker: "
            + tracker.getTrackerName());
      }
    }

    // If no tasks were found, return null
    return tasks.isEmpty() ? null : tasks;
  }

 
  private boolean needLaunchTask(YTTaskType taskType, JobInfo info) {
    if (taskType == YTTaskType.MAP) {
      return info.runningMaps < info.mapFairShare;
    } else {
      return info.runningReduces < info.reduceFairShare;
    }    
  }
  
  /**
   * Get the maximum locality level at which a given job is allowed to launch
   * tasks, based on how long it has been waiting for local tasks. This is used
   * to implement the "delay scheduling" feature of the Fair Scheduler for
   * optimizing data locality. If the job has no locality information (e.g. it
   * does not use HDFS), this method returns LocalityLevel.ANY, allowing tasks
   * at any level. Otherwise, the job can only launch tasks at its current
   * locality level or lower, unless it has waited at least localityDelay
   * milliseconds (in which case it can go one level beyond) or 2 *
   * localityDelay millis (in which case it can go to any level).
   */
  protected LocalityLevel getAllowedLocalityLevel(JobInProgress job,
      JobInfo info, long currentTime) {
    if (info == null) { // Job not in infos (shouldn't happen)
      LOG.error("getAllowedLocalityLevel called on job " + job
          + ", which does not have a JobInfo in infos");
      return LocalityLevel.ANY;
    }
    if (job.nonLocalMaps.size() > 0) { // Job doesn't have locality information
      return LocalityLevel.ANY;
    }
    // In the common case, compute locality level based on time waited
    switch (info.lastMapLocalityLevel) {
    case NODE: // Last task launched was node-local
      if (info.timeWaitedForLocalMap >= 2 * localityDelay)
        return LocalityLevel.ANY;
      else if (info.timeWaitedForLocalMap >= localityDelay)
        return LocalityLevel.RACK;
      else
        return LocalityLevel.NODE;
    case RACK: // Last task launched was rack-local
      if (info.timeWaitedForLocalMap >= localityDelay)
        return LocalityLevel.ANY;
      else
        return LocalityLevel.RACK;
    default: // Last task was non-local; can launch anywhere
      return LocalityLevel.ANY;
    }
  }

  /**
   * Recompute the internal variables used by the scheduler - per-job weights,
   * fair shares, deficits, minimum slot allocations, and numbers of running
   * and needed tasks of each type. 
   */
  protected void update() {
    
    // Recompute locality delay from JobTracker heartbeat interval if enabled.
    // This will also lock the JT, so do it outside of a fair scheduler lock.
    if (autoComputeLocalityDelay && tickCount % 600 == 0) {
      if (taskTrackerManager instanceof JobTracker) {
        JobTracker jobTracker = (JobTracker) taskTrackerManager;
        localityDelay = Math.min(MAX_AUTOCOMPUTED_LOCALITY_DELAY,
            (long) (1.5 * jobTracker.getNextHeartbeatInterval()));
      }
    }
    
    synchronized (this) {
      // Remove non-running jobs
      for (YTPool pool : poolMgr.getPools()) {
        List<JobInProgress> toRemove = new ArrayList<JobInProgress>();
        for (JobInProgress job : pool.getJobs()) {
          int runState = job.getStatus().getRunState();
          if (runState == JobStatus.SUCCEEDED || runState == JobStatus.FAILED
              || runState == JobStatus.KILLED) {
            toRemove.add(job);
          }
        }
        for (JobInProgress job : toRemove) {
          pool.removeJob(job);
        }
      }

      updateRunnability();
      updateTaskCounts();
      updateWeights();
      updateMinSlots();
      updateUpdateInterval();
      updateLocalityWaitTimes(clock.getTime());
      updateRunnableMapAndReduce();

      // Reload allocations file if it hasn't been loaded in a while
      poolMgr.reloadAllocsIfNecessary(taskTrackerManager);
      if (tickCount % 600 == 0) {
        reloadConfiguration(new Configuration(true));
      }
    }
  }
  
  private void updateRunnableMapAndReduce() {
    totalMapSlots = getTotalSlots(YTTaskType.MAP);
    totalReduceSlots = getTotalSlots(YTTaskType.REDUCE);
    
    runnableReduces = 0;
    runnableMaps = 0;
    for (YTPool pool: poolMgr.getPools()) {
      for (JobInProgress job: pool.getRunnableJobs())  {
        runnableMaps += pool.runnableTasks(job, YTTaskType.MAP);
        runnableReduces += pool.runnableTasks(job, YTTaskType.REDUCE);
      }
    }    
  }
  
  private void updateRunnability() {
    // Start by marking everything as not runnable
    for (YTPool pool: poolMgr.getPools()) {
      for (JobInProgress job: pool.getRunnableJobs()) {
        JobInfo info = pool.getJobInfo(job);
        if (info != null) {
          info.runnable = false;          
        }
      }
    }
    
    for (YTPool pool : poolMgr.getPools()) {
      Map<String, Integer> userJobs = new HashMap<String, Integer>();
      int cntPoolJobs = 0;
      int cntPoolMaxJobs = poolMgr.getPoolMaxJobs(pool.getName());
      List<JobInProgress> nextRunnableJobs = new ArrayList<JobInProgress>(
          cntPoolMaxJobs);
      for (JobInProgress job: pool.getOrderedJobs()) {
        if (job.getStatus().getRunState() == JobStatus.RUNNING) {
          String user = job.getJobConf().getUser();
          int userCount = userJobs.containsKey(user) ? userJobs.get(user) : 0;
          if (userCount < poolMgr.getUserMaxJobs(user)
              && cntPoolJobs < cntPoolMaxJobs) {
            pool.getJobInfo(job).runnable = true;
            userJobs.put(user, userCount + 1);
            // make sure there are maxRunningJobs jobs can run maps if there are more then maxRunningJobs jobs in pool
            if (job.finishedMapTasks < job.numMapTasks) {
              cntPoolJobs ++;
            }
            nextRunnableJobs.add(job);
          } else if (cntPoolJobs == cntPoolMaxJobs) {
            break;
          }
        }
      }
      pool.setRunnableJobs(nextRunnableJobs);
    }
  }

  private void updateTaskCounts() {
    for (YTPool pool : poolMgr.getPools()) {
      int totalRunningMaps = 0;
      int totalRunningReduces = 0;
      
      int maxPoolRunningJobs = poolMgr.getPoolMaxJobs(pool.getName());
      int needReduceJobs = 0;
            
      for (JobInProgress job: pool.getOrderedJobs()) {        
        JobInfo info = pool.getJobInfo(job);

        if (job.getStatus().getRunState() != JobStatus.RUNNING)
          continue; // Job is still in PREP state and tasks aren't initialized

        if (info.runnable == false && info.runningMaps == 0
            && info.runningReduces == 0)
          continue;

        int totalMaps = job.numMapTasks;
        int finishedMaps = 0;
        int runningMaps = 0;
        int runningMapsWithoutSpeculative = 0;

        // Count maps
        if (info.runnable || info.runningMaps != 0) {
          for (TaskInProgress tip : job.getMapTasks()) {
            if (tip.isComplete()) {
              finishedMaps += 1;
            } else if (tip.isRunning()) {
              runningMaps += tip.getActiveTasks().size();
              runningMapsWithoutSpeculative += 1;
            }
          }
          info.runningMaps = runningMaps;
          info.neededMaps = (totalMaps - runningMapsWithoutSpeculative - finishedMaps + taskSelector
              .neededSpeculativeMaps(job));
        }

        // Count reduces
        if (info.runnable || info.runningReduces != 0) {
          int totalReduces = job.numReduceTasks;
          int finishedReduces = 0;
          int runningReduces = 0;
          int runningReducesWithoutSpeculative = 0;
          for (TaskInProgress tip : job.getReduceTasks()) {
            if (tip.isComplete()) {
              finishedReduces += 1;
            } else if (tip.isRunning()) {
              runningReduces += tip.getActiveTasks().size();
              runningReducesWithoutSpeculative += 1;
            }
          }
          info.runningReduces = runningReduces;
          // make sure there are only maxRunningJobs in pool can run reduce
          if (needReduceJobs < maxPoolRunningJobs && 
              enoughMapsFinishedToRunReduces(finishedMaps, totalMaps, pool.getName(), info.jobLevel)) {
            info.neededReduces = (totalReduces - runningReducesWithoutSpeculative
                - finishedReduces + taskSelector.neededSpeculativeReduces(job));
            needReduceJobs += 1;
          } else {
            info.neededReduces = 0;
          }
        }

        if (!info.runnable) {
          info.neededMaps = 0;
          info.neededReduces = 0;
        }

        totalRunningMaps += info.runningMaps;
        totalRunningReduces += info.runningReduces;
      }
      pool.setTotalRunningMaps(totalRunningMaps);
      pool.setTotalRunningReduces(totalRunningReduces);
    }
  }

  /**
   * Has a job finished enough maps to allow launching its reduces?
   * HADOOP-4666
   */
  protected boolean enoughMapsFinishedToRunReduces(
      int finishedMaps, int totalMaps) {
    if (waitForMapsBeforeLaunchingReduces) {
      return finishedMaps >= Math.max(1, totalMaps * 0.1);
    } else {
      return true;
    }
  }
  
  /**
   * Has a job finished enough maps to allow launching its reduces 
   * according to its job level and poolname?
   */
  protected boolean enoughMapsFinishedToRunReduces(
      int finishedMaps, int totalMaps, String poolname, int jobLevel) {
    if (!waitForMapsBeforeLaunchingReduces) { 
      return true;
    }
    
    YTPoolManager.ReduceWatcher watcher = poolMgr.getReduceWatcher(poolname);
    if (watcher == null) {
      return enoughMapsFinishedToRunReduces(finishedMaps, totalMaps);
    }
    return watcher.shouldRunReduces(finishedMaps, totalMaps, jobLevel);
  }
  
  private void updateWeights() {
    for (YTPool pool: poolMgr.getPools()) {     
      for (Map.Entry<JobInProgress, JobInfo> entry: pool.getJobInfos().entrySet()) {
        JobInProgress job = entry.getKey();
        JobInfo info = entry.getValue();
        info.mapWeight = calculateWeight(job, YTTaskType.MAP, info, pool);
        info.reduceWeight = calculateWeight(job, YTTaskType.REDUCE, info, pool);
      }
    }
  }
  
  private void updateMinSlotsByFIFO(YTPool pool) {
    for (final YTTaskType type: YTTaskType.values()) {
      int slotsLeft = poolMgr.getAllocation(pool.getName(), type);
      for (JobInProgress job: pool.getRunnableJobs()) {
        if (slotsLeft <= 0) {
          break;
        }
        slotsLeft = giveMinSlots(job, type, slotsLeft, slotsLeft, pool);
      }
    }
  }
  
  private void updateMinSlots() {
    // For each pool, distribute its task allocation among jobs in it that need
    // slots. This is a little tricky since some jobs in the pool might not be
    // able to use all the slots, e.g. they might have only a few tasks left.
    // To deal with this, we repeatedly split up the available task slots
    // between the jobs left, give each job min(its alloc, # of slots it needs),
    // and redistribute any slots that are left over between jobs that still
    // need slots on the next pass. If, in total, the jobs in our pool don't
    // need all its allocation, we leave the leftover slots for general use.
    for (YTPool pool : poolMgr.getPools()) {
      // Clear old minSlots
      for (JobInfo info : pool.getJobInfos().values()) {
        info.mapFairShare = 0;
        info.reduceFairShare = 0;
      }

      if (poolMgr.getPoolUseFIFO(pool.getName())) {
        updateMinSlotsByFIFO(pool);
        continue;
      }
      
      for (final YTTaskType type : YTTaskType.values()) {
        List<JobInProgress> jobs = new LinkedList<JobInProgress>(pool.getRunnableJobs());
        int slotsLeft = poolMgr.getAllocation(pool.getName(), type);
        // Keep assigning slots until none are left
        while (slotsLeft > 0) {
          // Figure out total weight of the highest joblevel jobs that still
          // need slots
          double totalWeight = 0;

          int topJobLevel = -1;
          ArrayList<JobInProgress> jobsOftopJobLevel = new ArrayList<JobInProgress>();

          for (Iterator<JobInProgress> it = jobs.iterator(); it.hasNext();) {
            JobInProgress job = it.next();
            JobInfo info = pool.getJobInfo(job);
            if (pool.isRunnable(job) && pool.runnableTasks(job, type) > pool.minTasks(job, type)) {
              if (info.jobLevel > topJobLevel) {
                topJobLevel = info.jobLevel;
                totalWeight = pool.weight(job, type);
                jobsOftopJobLevel.clear();
                jobsOftopJobLevel.add(job);
              } else if (info.jobLevel == topJobLevel) {
                totalWeight += pool.weight(job, type);
                jobsOftopJobLevel.add(job);
              }
            } else {
              it.remove();
            }
          }

          if (totalWeight == 0) // No jobs that can use more slots are left
            break;
          // Assign slots to jobs, using the floor of their weight divided by
          // total weight. This ensures that all jobs get some chance to take
          // a slot. Then, if no slots were assigned this way, we do another
          // pass where we use ceil, in case some slots were still left over.
          int oldSlots = slotsLeft; // Copy slotsLeft so we can modify it
          for (JobInProgress job : jobsOftopJobLevel) {
            double weight = pool.weight(job, type);
            int share = (int) Math.floor(oldSlots * weight / totalWeight);
            slotsLeft = giveMinSlots(job, type, slotsLeft, share, pool);
          }
          if (slotsLeft == oldSlots) {
            for (JobInProgress job : jobsOftopJobLevel) {
              double weight = pool.weight(job, type);
              int share = (int) Math.ceil(oldSlots * weight / totalWeight);
              slotsLeft = giveMinSlots(job, type, slotsLeft, share, pool);
            }
          }
        }
      }
    }
  }
 
  /**
   * Give up to <code>tasksToGive</code> min slots to a job (potentially fewer
   * if either the job needs fewer slots or there aren't enough slots left).
   * Returns the number of slots left over.
   */
  private int giveMinSlots(JobInProgress job, YTTaskType type,
      int slotsLeft, int slotsToGive, YTPool pool) {
    int runnable = pool.runnableTasks(job, type);
    int curMin = pool.minTasks(job, type);
    slotsToGive = Math.min(Math.min(slotsLeft, runnable - curMin), slotsToGive);
    slotsLeft -= slotsToGive;
    JobInfo info = pool.getJobInfo(job);
    if (type == YTTaskType.MAP)
      info.mapFairShare += slotsToGive;
    else
      info.reduceFairShare += slotsToGive;
    return slotsLeft;
  }

  private double calculateWeight(JobInProgress job, YTTaskType taskType, JobInfo jobInfo, YTPool pool) {
    if (!jobInfo.runnable) {
      return 0;
    } else {
      double weight = 1.0;
      if (sizeBasedWeight) {
        // Set weight based on runnable tasks
        weight = Math.log1p(pool.runnableTasks(job, taskType)) / Math.log(2);
      }
      weight *= getPriorityFactor(job.getPriority());
      if (weightAdjuster != null) {
        // Run weight through the user-supplied weightAdjuster
        weight = weightAdjuster.adjustWeight(job, taskType, weight);
      }      
      return weight;
    }
  }

  
  
  public YTPoolManager getPoolManager() {
    return poolMgr;
  }

  public int getTotalSlots(YTTaskType type) {
    int slots = 0;
    for (TaskTrackerStatus tt: taskTrackerManager.taskTrackers()) {
      slots += (type == YTTaskType.MAP ?
          tt.getMaxMapTasks() : tt.getMaxReduceTasks());
    }
    return slots;
  }

  @Override
  public synchronized Collection<JobInProgress> getJobs(String queueName) {
    YTPool myJobPool = poolMgr.getPool(queueName);
    return myJobPool.getJobs();
  }
  
  private double getPriorityFactor(JobPriority priority) {
    switch (priority) {
    case VERY_HIGH: return 4.0;
    case HIGH:      return 2.0;
    case NORMAL:    return 1.0;
    case LOW:       return 0.5;
    default:        return 0.25; //priority = VERY_LOW
    }
  }
  
  public JobInfo getJobInfo(JobInProgress job) {
    String poolName = poolMgr.getPoolName(job);
    if (poolName != null) {
      YTPool pool = poolMgr.getPool(poolName);
      return pool.getJobInfo(job);
    }
    return null;    
  }
  
  /**
   * Get maximum number of tasks to assign on a TaskTracker on a heartbeat. The
   * scheduler may launch fewer than this many tasks if the LoadManager says not
   * to launch more, but it will never launch more than this number.
   */
  private int maxTasksToAssign(YTTaskType type, TaskTrackerStatus tts) {
    if (!assignMultiple)
      return 1;
    int cap = (type == YTTaskType.MAP) ? mapAssignCap : reduceAssignCap;
    int available = (type == YTTaskType.MAP) ? tts.getAvailableMapSlots(): tts.getAvailableReduceSlots();
    if (cap == -1) // Infinite cap; use the TaskTracker's slot count
      return available;
    else
      return Math.min(cap, available);
  }

  /**
   * Update locality wait times for jobs that were skipped at last heartbeat.
   */
  private void updateLocalityWaitTimes(long currentTime) {
    long timeSinceLastHeartbeat = (lastHeartbeatTime == 0 ? 0 : currentTime
        - lastHeartbeatTime);
    lastHeartbeatTime = currentTime;

    for (YTPool pool : poolMgr.getPools()) {
      for (JobInProgress job : pool.getRunnableJobs()) {
        JobInfo info = pool.getJobInfo(job);
        // Update wait time only if the job is skipped and not assigned even
        // once between updates
        if (info.skippedAtLastHeartbeat > 0
            && info.assignedAtLastHeartbeat == 0) {
          info.timeWaitedForLocalMap += timeSinceLastHeartbeat;
        }
        info.assignedAtLastHeartbeat = 0;
        info.skippedAtLastHeartbeat = 0;
      }
    }
  }

  /**
   * Update a job's locality level and locality wait variables given that that
   * it has just launched a map task on a given task tracker.
   */
  private void updateLastMapLocalityLevel(JobInProgress job, JobInfo info,
      Task mapTaskLaunched, TaskTrackerStatus tracker) {
    LocalityLevel localityLevel = LocalityLevel.fromTask(job, mapTaskLaunched,
        tracker);
    info.lastMapLocalityLevel = localityLevel;
    info.timeWaitedForLocalMap = 0;
    // eventLog.log("ASSIGNED_LOC_LEVEL", job.getJobID(), localityLevel);
  }
  
  /**
   * Reload configuration periodically to prevent restarting
   * <code>JobTracker</code> when configuration is changed.
   */
  private void reloadConfiguration(Configuration conf) {
    assignMultiple = conf.getBoolean("mapred.yunti3scheduler.assignmultiple", true);
    mapAssignCap = conf.getInt("mapred.yunti3scheduler.assignmultiple.maps", 1);
    reduceAssignCap = conf.getInt("mapred.yunti3scheduler.assignmultiple.reduces", 1);
    
    localityDelay = conf.getLong("mapred.yunti3scheduler.locality.delay", -1);
    if (localityDelay == -1)
      autoComputeLocalityDelay = true; // Compute from heartbeat interval
          
    sizeBasedWeight = conf.getBoolean("mapred.yunti3scheduler.sizebasedweight",
        false);
  }
}
