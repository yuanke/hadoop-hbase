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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A {@link TaskScheduler} that implements fair sharing.
 */
public class FairScheduler extends TaskScheduler {
  /** How often fair shares are re-calculated */
  public static long UPDATE_INTERVAL = 500;
  public static final Log LOG = LogFactory.getLog(
      "org.apache.hadoop.mapred.FairScheduler");
  
  public static final Log Logger = LogFactory.getLog("fairScheduler");
  
  protected PoolManager poolMgr;
  
  protected LoadManager loadMgr;
  protected TaskSelector taskSelector;
  protected WeightAdjuster weightAdjuster; // Can be null for no weight adjuster
  protected Map<JobInProgress, JobInfo> infos = // per-job scheduling variables
    new HashMap<JobInProgress, JobInfo>();
  protected long lastUpdateTime;           // Time when we last updated infos
  protected boolean initialized;  // Are we initialized?
  protected boolean running;      // Are we running?
  protected boolean useFifo;      // Set if we want to revert to FIFO behavior
  protected boolean assignMultiple; // Simultaneously assign map and reduce?
  protected boolean sizeBasedWeight; // Give larger weights to larger jobs
  protected boolean waitForMapsBeforeLaunchingReduces = true;
  private Clock clock;
  private boolean runBackgroundUpdates; // Can be set to false for testing
  private EagerTaskInitializationListener eagerInitListener;
  private JobListener jobListener; 
  
  /**
   * A class for holding per-job scheduler variables. These always contain the
   * values of the variables at the last update(), and are used along with a
   * time delta to update the map and reduce deficits before a new update().
   */
  static class JobInfo {
    boolean runnable = false;   // Can the job run given user/pool limits?
    boolean product = false;	// Is the job is prodcut job?
    double mapWeight = 0;       // Weight of job in calculation of map share
    double reduceWeight = 0;    // Weight of job in calculation of reduce share
    long mapDeficit = 0;        // Time deficit for maps
    long reduceDeficit = 0;     // Time deficit for reduces
    int runningMaps = 0;        // Maps running at last update
    int runningReduces = 0;     // Reduces running at last update
    int neededMaps;             // Maps needed at last update
    int neededReduces;          // Reduces needed at last update
    int minMaps = 0;            // Minimum maps as guaranteed by pool
    int minReduces = 0;         // Minimum reduces as guaranteed by pool
    double mapFairShare = 0;    // Fair share of map slots at last update
    double reduceFairShare = 0; // Fair share of reduce slots at last update
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
    private Pool pool;
    private PoolManager poolMgr;
    private QueueManager queueMgr;

    public SchedulingInfo(Pool pool, PoolManager poolMgr, QueueManager queueMgr) {
      this.pool = pool;
      this.poolMgr = poolMgr;
      this.queueMgr = queueMgr;
    }

    @Override
    public String toString(){
      int runningJobs = 0;
      int maxJobs = 0;
      int runningMaps = 0;
      int runningReduces = 0;
      int minMaps = 0;
      int minReduces = 0;

      runningJobs = pool.getJobs().size();
      for (JobInProgress job : pool.getJobs()) {
        runningMaps += job.runningMaps();
        runningReduces += job.runningReduces();
      }

      synchronized (poolMgr) {
        maxJobs = poolMgr.getPoolMaxJobs(pool.getName());
        minMaps = poolMgr.getAllocation(pool.getName(), TaskType.MAP);
        minReduces = poolMgr.getAllocation(pool.getName(), TaskType.REDUCE);
      }

      StringBuffer sb = new StringBuffer();
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

  public FairScheduler() {
    this(new Clock(), true);
  }
  
  /**
   * Constructor used for tests, which can change the clock and disable updates.
   */
  protected FairScheduler(Clock clock, boolean runBackgroundUpdates) {
    this.clock = clock;
    this.runBackgroundUpdates = runBackgroundUpdates;
    this.jobListener = new JobListener();
  }

  @Override
  public void start() {
    try {
      Configuration conf = getConf();
      this.eagerInitListener = new EagerTaskInitializationListener();
      eagerInitListener.setTaskTrackerManager(taskTrackerManager);
      eagerInitListener.start();
      taskTrackerManager.addJobInProgressListener(eagerInitListener);
      taskTrackerManager.addJobInProgressListener(jobListener);
      poolMgr = new PoolManager(conf);
      loadMgr = (LoadManager) ReflectionUtils.newInstance(
          conf.getClass("mapred.fairscheduler.loadmanager", 
              CapBasedLoadManager.class, LoadManager.class), conf);
      loadMgr.setTaskTrackerManager(taskTrackerManager);
      loadMgr.start();
      taskSelector = (TaskSelector) ReflectionUtils.newInstance(
          conf.getClass("mapred.fairscheduler.taskselector", 
              DefaultTaskSelector.class, TaskSelector.class), conf);
      taskSelector.setTaskTrackerManager(taskTrackerManager);
      taskSelector.start();
      Class<?> weightAdjClass = conf.getClass(
          "mapred.fairscheduler.weightadjuster", null);
      if (weightAdjClass != null) {
        weightAdjuster = (WeightAdjuster) ReflectionUtils.newInstance(
            weightAdjClass, conf);
      }
      assignMultiple = conf.getBoolean("mapred.fairscheduler.assignmultiple",
          true);
      sizeBasedWeight = conf.getBoolean("mapred.fairscheduler.sizebasedweight",
          false);

      initialized = true;
      running = true;
      lastUpdateTime = clock.getTime();
      // Start a thread to update deficits every UPDATE_INTERVAL
      if (runBackgroundUpdates)
        new UpdateThread().start();
      // Register servlet with JobTracker's Jetty server
      if (taskTrackerManager instanceof JobTracker) {
        JobTracker jobTracker = (JobTracker) taskTrackerManager;
        StatusHttpServer infoServer = jobTracker.infoServer;
        infoServer.setAttribute("scheduler", this);
        infoServer.addServlet("scheduler", "/scheduler",
            FairSchedulerServlet.class);
      }
      // set scheduling information to queue manager. added by liangly
      setSchedulerInfo();
    } catch (Exception e) {
      // Can't load one of the managers - crash the JobTracker now while it is
      // starting up so that the user notices.
      throw new RuntimeException("Failed to start FairScheduler", e);
    }
    LOG.info("Successfully configured FairScheduler");
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
        Pool pool = poolMgr.getPool(queueName);
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
      synchronized (FairScheduler.this) {
        poolMgr.addJob(job);
        JobInfo info = new JobInfo();
        infos.put(job, info);
        update();
      }
    }
    
    @Override
    public void jobRemoved(JobInProgress job) {
      synchronized (FairScheduler.this) {
        poolMgr.removeJob(job);
        infos.remove(job);
      }
    }
  
    @Override
    public void jobUpdated(JobChangeEvent event) {
    }
  }

  /**
   * A thread which calls {@link FairScheduler#update()} ever
   * <code>UPDATE_INTERVAL</code> milliseconds.
   */
  private class UpdateThread extends Thread {
    private UpdateThread() {
      super("FairScheduler update thread");
    }

    public void run() {
      while (running) {
        try {
          Thread.sleep(UPDATE_INTERVAL);
          update();
          reclaimCapacity();
        } catch (Exception e) {
          LOG.error("Failed to update fair share calculations", e);
        }
      }
    }
  }
  
    private boolean isUseOtherSlots(JobInProgress job) {
         boolean queueUseOtherSlots = true;
         if(taskTrackerManager instanceof JobTracker) {
                 JobTracker jobTracker = (JobTracker) taskTrackerManager;
                 queueUseOtherSlots = jobTracker.isQueueUseOtherSlots(job.getJobConf().getQueueName());
         }
         boolean jobUseOtherSlots = job.getJobConf().isJobUseOtherSlots();
         return (queueUseOtherSlots && jobUseOtherSlots);
   }
    
    private void updateUpdateInterval(){
    	if(taskTrackerManager instanceof JobTracker) {
    		JobTracker jobTracker = (JobTracker) taskTrackerManager;
    		UPDATE_INTERVAL = jobTracker.getJobSlotsUpdateInterval();
    	}
    }

   //add by weichao
   private boolean canAssignTask(JobInProgress job, TaskType type) {
         if(isUseOtherSlots(job))
                 return true;
         Pool pool = poolMgr.getPool(poolMgr.getPoolName(job));
         if(needLocalReclaim(pool, type) && infos.get(job).product)
                 return true;

         int curPoolAllocation = 0;
         for(JobInProgress jobInProgress : pool.getJobs()) {
                 JobInfo jobInfo = infos.get(jobInProgress);
                 curPoolAllocation += (type == TaskType.MAP ? jobInfo.runningMaps : jobInfo.runningReduces);
         }
         int poolAllocation = poolMgr.getAllocation(pool.getName(), type);
         return (poolAllocation >= curPoolAllocation + 1);
   }


  
  @Override
  public synchronized List<Task> assignTasks(TaskTrackerStatus tracker)
      throws IOException {
    if (!initialized) // Don't try to assign tasks if we haven't yet started up
      return null;
    
    // Reload allocations file if it hasn't been loaded in a while
    poolMgr.reloadAllocsIfNecessary(taskTrackerManager);
    
    // Compute total runnable maps and reduces
    int runnableMaps = 0;
    int runnableReduces = 0;
    for (JobInProgress job: infos.keySet()) {
      runnableMaps += runnableTasks(job, TaskType.MAP);
      runnableReduces += runnableTasks(job, TaskType.REDUCE);
    }

    // Compute total map/reduce slots
    // In the future we can precompute this if the Scheduler becomes a 
    // listener of tracker join/leave events.
    int totalMapSlots = getTotalSlots(TaskType.MAP);
    int totalReduceSlots = getTotalSlots(TaskType.REDUCE);
    
    // Scan to see whether any job needs to run a map, then a reduce
    ArrayList<Task> tasks = new ArrayList<Task>();
    TaskType[] types = new TaskType[] {TaskType.MAP, TaskType.REDUCE};
    for (TaskType taskType: types) {
      boolean canAssign = (taskType == TaskType.MAP) ? 
          loadMgr.canAssignMap(tracker, runnableMaps, totalMapSlots) :
          loadMgr.canAssignReduce(tracker, runnableReduces, totalReduceSlots);
      if (canAssign) {
        // Figure out the jobs that need this type of task
        List<JobInProgress> candidates = new ArrayList<JobInProgress>();
        for (JobInProgress job: infos.keySet()) {
          if (job.getStatus().getRunState() == JobStatus.RUNNING && 
        		  neededTasks(job, taskType) > 0 && canAssignTask(job, taskType)) {
            candidates.add(job);
          }
        }
        // Sort jobs by deficit (for Fair Sharing) or submit time (for FIFO)
        Comparator<JobInProgress> comparator = useFifo ?
            new FifoJobComparator() : new DeficitComparator(taskType);
        Collections.sort(candidates, comparator);
        for (JobInProgress job: candidates) {
          Task task = (taskType == TaskType.MAP ? 
              taskSelector.obtainNewMapTask(tracker, job) :
              taskSelector.obtainNewReduceTask(tracker, job));
          if (task != null){// && task.getTaskID().toString().endsWith("_0")) {
        	  //Logger.debug("-------------- " + task.getTaskID().toString() + " --------------------");
            // Update the JobInfo for this job so we account for the launched
            // tasks during this update interval and don't try to launch more
            // tasks than the job needed on future heartbeats
            JobInfo info = infos.get(job);
            if (taskType == TaskType.MAP) {
              info.runningMaps++;
              info.neededMaps--;
            } else {
              info.runningReduces++;
              info.neededReduces--;
            }
            tasks.add(task);
            if (!assignMultiple)
              return tasks;
            break;
          }else if(task != null){
        	  //Logger.debug("++++++++++++++ " + task.getTaskID().toString() + " ++++++++++++++++");
          }
        }
      }
    }
    
    // If no tasks were found, return null
    return tasks.isEmpty() ? null : tasks;
  }

  /**
   * Compare jobs by deficit for a given task type, putting jobs whose current
   * allocation is less than their minimum share always ahead of others. This is
   * the default job comparator used for Fair Sharing.
   */
  private class DeficitComparator implements Comparator<JobInProgress> {
    private final TaskType taskType;

    private DeficitComparator(TaskType taskType) {
      this.taskType = taskType;
    }

    public int compare(JobInProgress j1, JobInProgress j2) {
      // Put needy jobs ahead of non-needy jobs (where needy means must receive
      // new tasks to meet slot minimum), comparing among jobs of the same type
      // by deficit so as to put jobs with higher deficit ahead.
	  
    	if(!isUseOtherSlots(j1) && isUseOtherSlots(j2))
		    return -1;
    	else if(isUseOtherSlots(j1) && !isUseOtherSlots(j2))
		    return 1;
    	
      JobInfo j1Info = infos.get(j1);
      JobInfo j2Info = infos.get(j2);
      long deficitDif;
      boolean j1Needy, j2Needy;
      if (taskType == TaskType.MAP) {
        j1Needy = j1.runningMaps() < Math.floor(j1Info.minMaps);
        j2Needy = j2.runningMaps() < Math.floor(j2Info.minMaps);
        deficitDif = j2Info.mapDeficit - j1Info.mapDeficit;
      } else {
        j1Needy = j1.runningReduces() < Math.floor(j1Info.minReduces);
        j2Needy = j2.runningReduces() < Math.floor(j2Info.minReduces);
        deficitDif = j2Info.reduceDeficit - j1Info.reduceDeficit;
      }
      if (j1Needy && !j2Needy)
        return -1;
      else if (j2Needy && !j1Needy)
        return 1;
      else // Both needy or both non-needy; compare by deficit
        return (int) Math.signum(deficitDif);
    }
  }
  
  /**
   * Recompute the internal variables used by the scheduler - per-job weights,
   * fair shares, deficits, minimum slot allocations, and numbers of running
   * and needed tasks of each type. 
   */
  protected synchronized void update() {
    // Remove non-running jobs
    List<JobInProgress> toRemove = new ArrayList<JobInProgress>();
    for (JobInProgress job: infos.keySet()) { 
      int runState = job.getStatus().getRunState();
      if (runState == JobStatus.SUCCEEDED || runState == JobStatus.FAILED
          || runState == JobStatus.KILLED) {
        toRemove.add(job);
      }
    }
    for (JobInProgress job: toRemove) {
      infos.remove(job);
      poolMgr.removeJob(job);
    }
    // Update running jobs with deficits since last update, and compute new
    // slot allocations, weight, shares and task counts
    long now = clock.getTime();
    long timeDelta = now - lastUpdateTime;
    updateDeficits(timeDelta);
    updateRunnability();
    updateTaskCounts();
    updateWeights();
    updateMinSlots();
    updateFairShares();
    updateUpdateInterval();
    lastUpdateTime = now;
  }
  
  private void updateDeficits(long timeDelta) {
    for (JobInfo info: infos.values()) {
      info.mapDeficit +=
        (info.mapFairShare - info.runningMaps) * timeDelta;
      info.reduceDeficit +=
        (info.reduceFairShare - info.runningReduces) * timeDelta;
    }
  }
  
  private void updateRunnability() {
    // Start by marking everything as not runnable
    for (JobInfo info: infos.values()) {
      info.runnable = false;
    }
    // Create a list of sorted jobs in order of start time and priority
    List<JobInProgress> jobs = new ArrayList<JobInProgress>(infos.keySet());
    Collections.sort(jobs, new FifoJobComparator());
    // Mark jobs as runnable in order of start time and priority, until
    // user or pool limits have been reached.
    Map<String, Integer> userJobs = new HashMap<String, Integer>();
    Map<String, Integer> poolJobs = new HashMap<String, Integer>();
    for (JobInProgress job: jobs) {
      if (job.getStatus().getRunState() == JobStatus.RUNNING) {
        String user = job.getJobConf().getUser();
        String pool = poolMgr.getPoolName(job);
        int userCount = userJobs.containsKey(user) ? userJobs.get(user) : 0;
        int poolCount = poolJobs.containsKey(pool) ? poolJobs.get(pool) : 0;
        if (userCount < poolMgr.getUserMaxJobs(user) && 
            poolCount < poolMgr.getPoolMaxJobs(pool)) {
          infos.get(job).runnable = true;
          userJobs.put(user, userCount + 1);
          poolJobs.put(pool, poolCount + 1);
        }
      }
    }
  }

  private void updateTaskCounts() {
	  for (Map.Entry<JobInProgress, JobInfo> entry: infos.entrySet()) {
	      JobInProgress job = entry.getKey();
	      JobInfo info = entry.getValue();
	      if (job.getStatus().getRunState() != JobStatus.RUNNING)
	        continue; // Job is still in PREP state and tasks aren't initialized
	      // Count maps
	      int totalMaps = job.numMapTasks;
	      int finishedMaps = 0;
	      int runningMaps = 0;
	      for (TaskInProgress tip: job.getMapTasks()) {
	        if (tip.isComplete()) {
	          finishedMaps += 1;
	        } else if (tip.isRunning()) {
	          runningMaps += tip.getActiveTasks().size();
	        }
	      }
	      info.runningMaps = runningMaps;
	      info.neededMaps = (totalMaps - runningMaps - finishedMaps
	          + taskSelector.neededSpeculativeMaps(job));
	      // Count reduces
	      int totalReduces = job.numReduceTasks;
	      int finishedReduces = 0;
	      int runningReduces = 0;
	      for (TaskInProgress tip: job.getReduceTasks()) {
	        if (tip.isComplete()) {
	          finishedReduces += 1;
	        } else if (tip.isRunning()) {
	          runningReduces += tip.getActiveTasks().size();
	        }
	      }
	      info.runningReduces = runningReduces;
      if (enoughMapsFinishedToRunReduces(finishedMaps, totalMaps)) {
        info.neededReduces = (totalReduces - runningReduces - finishedReduces 
                              + taskSelector.neededSpeculativeReduces(job));
      } else {
        info.neededReduces = 0;
      }
	      
	      // If the job was marked as not runnable due to its user or pool having
	      // too many active jobs, set the neededMaps/neededReduces to 0. We still
	      // count runningMaps/runningReduces however so we can give it a deficit.
	      if (!info.runnable) {
	        info.neededMaps = 0;
	        info.neededReduces = 0;
	      }
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
  
  private void updateWeights() {
    for (Map.Entry<JobInProgress, JobInfo> entry: infos.entrySet()) {
      JobInProgress job = entry.getKey();
      JobInfo info = entry.getValue();
      info.mapWeight = calculateWeight(job, TaskType.MAP);
      info.reduceWeight = calculateWeight(job, TaskType.REDUCE);
    }
  }
  
  private void updateMinSlots() {
    // Clear old minSlots
    for (JobInfo info: infos.values()) {
      info.minMaps = 0;
      info.minReduces = 0;
    }
    // For each pool, distribute its task allocation among jobs in it that need
    // slots. This is a little tricky since some jobs in the pool might not be
    // able to use all the slots, e.g. they might have only a few tasks left.
    // To deal with this, we repeatedly split up the available task slots
    // between the jobs left, give each job min(its alloc, # of slots it needs),
    // and redistribute any slots that are left over between jobs that still
    // need slots on the next pass. If, in total, the jobs in our pool don't
    // need all its allocation, we leave the leftover slots for general use.
    PoolManager poolMgr = getPoolManager();
    for (Pool pool: poolMgr.getPools()) {
      for (final TaskType type: TaskType.values()) {
        Set<JobInProgress> jobs = new HashSet<JobInProgress>(pool.getJobs());
        int slotsLeft = poolMgr.getAllocation(pool.getName(), type);
        // Keep assigning slots until none are left
        // for product jobs 
        while (slotsLeft > 0) {
          // Figure out total weight of jobs that still need slots
          double totalWeight = 0;
          for (Iterator<JobInProgress> it = jobs.iterator(); it.hasNext();) {
            JobInProgress job = it.next();
            if (isRunnable(job) && isProductJob(job) &&
                runnableTasks(job, type) > minTasks(job, type)) {
              totalWeight += weight(job, type);
            } else if (isProductJob(job)) { // only delete product job
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
          for (JobInProgress job: jobs) {
            if(!isProductJob(job)) {
            	continue;
            }
            double weight = weight(job, type);
            int share = (int) Math.floor(oldSlots * weight / totalWeight);
            slotsLeft = giveMinSlots(job, type, slotsLeft, share);
          }
          if (slotsLeft == oldSlots) {
            // No tasks were assigned; do another pass using ceil, giving the
            // extra slots to jobs in order of weight then deficit
            List<JobInProgress> sortedJobs = new ArrayList<JobInProgress>(jobs);
            Collections.sort(sortedJobs, new Comparator<JobInProgress>() {
              public int compare(JobInProgress j1, JobInProgress j2) {
                double dif = weight(j2, type) - weight(j1, type);
                if (dif == 0) // Weights are equal, compare by deficit 
                  dif = deficit(j2, type) - deficit(j1, type);
                return (int) Math.signum(dif);
              }
            });
            for (JobInProgress job: sortedJobs) {
              if(!isProductJob(job)) {
                continue;
              }
              double weight = weight(job, type);
              int share = (int) Math.ceil(oldSlots * weight / totalWeight);
              slotsLeft = giveMinSlots(job, type, slotsLeft, share);
            }
          }
        }
        
        double closeWeight=0;
        for (Iterator<JobInProgress> it = jobs.iterator(); it.hasNext();) {
          JobInProgress job = it.next();
          if(!isUseOtherSlots(job)) {
                  closeWeight += weight(job, type);
          }
        }

        for (Iterator<JobInProgress> it = jobs.iterator(); it.hasNext();) {
          JobInProgress job = it.next();
          if(!isUseOtherSlots(job)) {
                  slotsLeft = giveMinSlots(job, type, slotsLeft, (int)Math.floor(slotsLeft*weight(job, type) / closeWeight));
                  it.remove();
          }
        }

        
        // for non-prodution jobs 
        while (slotsLeft > 0) {
            // Figure out total weight of jobs that still need slots
            double totalWeight = 0;
            for (Iterator<JobInProgress> it = jobs.iterator(); it.hasNext();) {
              JobInProgress job = it.next();
              if (isRunnable(job) && !isProductJob(job) &&
                  runnableTasks(job, type) > minTasks(job, type)) {
                totalWeight += weight(job, type);
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
            for (JobInProgress job: jobs) {
              if(isProductJob(job)) {
                continue;
              }
              double weight = weight(job, type);
              int share = (int) Math.floor(oldSlots * weight / totalWeight);
              slotsLeft = giveMinSlots(job, type, slotsLeft, share);
            }
            if (slotsLeft == oldSlots) {
              // No tasks were assigned; do another pass using ceil, giving the
              // extra slots to jobs in order of weight then deficit
              List<JobInProgress> sortedJobs = new ArrayList<JobInProgress>(jobs);
              Collections.sort(sortedJobs, new Comparator<JobInProgress>() {
                public int compare(JobInProgress j1, JobInProgress j2) {
                  double dif = weight(j2, type) - weight(j1, type);
                  if (dif == 0) // Weights are equal, compare by deficit 
                    dif = deficit(j2, type) - deficit(j1, type);
                  return (int) Math.signum(dif);
                }
              });
              for (JobInProgress job: sortedJobs) {
                if(isProductJob(job)) {
                  continue;
                }
                double weight = weight(job, type);
                int share = (int) Math.ceil(oldSlots * weight / totalWeight);
                slotsLeft = giveMinSlots(job, type, slotsLeft, share);
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
  private int giveMinSlots(JobInProgress job, TaskType type,
      int slotsLeft, int slotsToGive) {
    int runnable = runnableTasks(job, type);
    int curMin = minTasks(job, type);
    slotsToGive = Math.min(Math.min(slotsLeft, runnable - curMin), slotsToGive);
    slotsLeft -= slotsToGive;
    JobInfo info = infos.get(job);
    if (type == TaskType.MAP)
      info.minMaps += slotsToGive;
    else
      info.minReduces += slotsToGive;
    return slotsLeft;
  }

  private void updateFairShares() {
    // Clear old fairShares
    for (JobInfo info: infos.values()) {
      info.mapFairShare = 0;
      info.reduceFairShare = 0;
    }
    // Assign new shares, based on weight and minimum share. This is done
    // as follows. First, we split up the available slots between all
    // jobs according to weight. Then if there are any jobs whose minSlots is
    // larger than their fair allocation, we give them their minSlots and
    // remove them from the list, and start again with the amount of slots
    // left over. This continues until all jobs' minSlots are less than their
    // fair allocation, and at this point we know that we've met everyone's
    // guarantee and we've split the excess capacity fairly among jobs left.
    for (TaskType type: TaskType.values()) {
      // Select only jobs that still need this type of task
      HashSet<JobInfo> jobsLeft = new HashSet<JobInfo>();
      for (Entry<JobInProgress, JobInfo> entry: infos.entrySet()) {
        JobInProgress job = entry.getKey();
        JobInfo info = entry.getValue();
        if (isRunnable(job) && runnableTasks(job, type) > 0) {
          info.product = isProductJob(job);
          jobsLeft.add(info);
        }
      }
      double slotsLeft = getTotalSlots(type);
      while (!jobsLeft.isEmpty()) {
        double totalWeight = 0;
        double productTotalWeight = 0;
        for (JobInfo info: jobsLeft) {
          double weight = (type == TaskType.MAP ?
              info.mapWeight : info.reduceWeight);
          totalWeight += weight;
          if(info.product) { 
            productTotalWeight += weight;
          }
        }
        
        boolean recomputeSlots = false;
        double oldSlots = slotsLeft; // Copy slotsLeft so we can modify it
        for (Iterator<JobInfo> iter = jobsLeft.iterator(); iter.hasNext();) {
          JobInfo info = iter.next();
          double minSlots = (type == TaskType.MAP ?
              info.minMaps : info.minReduces);
          double weight = (type == TaskType.MAP ?
              info.mapWeight : info.reduceWeight);
          double fairShare = weight / totalWeight * oldSlots;
          // TODO add by wu
          // product: (weight / productTotalWeight * oldSlots) is productFairShare
          // non-product: productTotalWeight ==0 => compare with fairShare 
          //              productTotalWeight > 0 => compare with 0
          if ((!info.product &&
                  ((productTotalWeight == 0 && minSlots > fairShare) ||
                                  (productTotalWeight > 0 && minSlots > 0)))
                  || info.product && minSlots > (weight / productTotalWeight * oldSlots)) {
            // Job needs more slots than its fair share; give it its minSlots,
            // remove it from the list, and set recomputeSlots = true to 
            // remember that we must loop again to redistribute unassigned slots
            if (type == TaskType.MAP)
              info.mapFairShare = minSlots;
            else
              info.reduceFairShare = minSlots;
            slotsLeft -= minSlots;
            iter.remove();
            recomputeSlots = true;
          }
        }
        if (!recomputeSlots) {
          // All minimums are met. Give each job its fair share of excess slots.
          
          // product job first
          double leftSlotsForNonProduct = oldSlots;
          for (JobInfo info: jobsLeft) {
            if(info.product) {
	            double weight = (type == TaskType.MAP ?
	                info.mapWeight : info.reduceWeight);
	            double fairShare = weight / productTotalWeight * oldSlots;
	            if (type == TaskType.MAP) {
	              int runnable = info.neededMaps + info.runningMaps;
	              info.mapFairShare = Math.min(fairShare, runnable);
	              leftSlotsForNonProduct -= info.mapFairShare;
	            } else {
	              int runnable = info.neededReduces + info.runningReduces;
	              info.reduceFairShare = Math.min(fairShare, runnable);
	              leftSlotsForNonProduct -= info.reduceFairShare;
	            }
            }
          }
          
          // non-product job 
          if(leftSlotsForNonProduct > 0.0) {
	          for (JobInfo info: jobsLeft) {
	              if(!info.product) {
	  	            double weight = (type == TaskType.MAP ?
	  	                info.mapWeight : info.reduceWeight);
	  	            double fairShare = 
	  	            	weight / (totalWeight - productTotalWeight) * leftSlotsForNonProduct;
	  	            if (type == TaskType.MAP)
	  	              info.mapFairShare = fairShare;
	  	            else
	  	              info.reduceFairShare = fairShare;
	              }
	          }
          }
          break;
        }
      }
    }
  }

  private double calculateWeight(JobInProgress job, TaskType taskType) {
    if (!isRunnable(job)) {
      return 0;
    } else {
      double weight = 1.0;
      if (sizeBasedWeight) {
        // Set weight based on runnable tasks
        weight = Math.log1p(runnableTasks(job, taskType)) / Math.log(2);
      }
      weight *= getPriorityFactor(job.getPriority());  
      if (weightAdjuster != null) {
        // Run weight through the user-supplied weightAdjuster
        weight = weightAdjuster.adjustWeight(job, taskType, weight);
      }
      return weight;
    }
  }

  private double getPriorityFactor(JobPriority priority) {
    switch (priority) {
    case VERY_HIGH: return 4.0;
    case HIGH:      return 2.0;
    case NORMAL:    return 1.0;
    case LOW:       return 0.5;
    default:        return 0.25; // priority = VERY_LOW
    }
  }
  
  public PoolManager getPoolManager() {
    return poolMgr;
  }

  public int getTotalSlots(TaskType type) {
    int slots = 0;
    for (TaskTrackerStatus tt: taskTrackerManager.taskTrackers()) {
      slots += (type == TaskType.MAP ?
          tt.getMaxMapTasks() : tt.getMaxReduceTasks());
    }
    return slots;
  }

  public boolean getUseFifo() {
    return useFifo;
  }
  
  public void setUseFifo(boolean useFifo) {
    this.useFifo = useFifo;
  }
  
  // Getter methods for reading JobInfo values based on TaskType, safely
  // returning 0's for jobs with no JobInfo present.

  protected int neededTasks(JobInProgress job, TaskType taskType) {
    JobInfo info = infos.get(job);
    if (info == null) return 0;
    return taskType == TaskType.MAP ? info.neededMaps : info.neededReduces;
  }
  
  protected int runningTasks(JobInProgress job, TaskType taskType) {
    JobInfo info = infos.get(job);
    if (info == null) return 0;
    return taskType == TaskType.MAP ? info.runningMaps : info.runningReduces;
  }

  protected int runnableTasks(JobInProgress job, TaskType type) {
    return neededTasks(job, type) + runningTasks(job, type);
  }

  protected int minTasks(JobInProgress job, TaskType type) {
    JobInfo info = infos.get(job);
    if (info == null) return 0;
    return (type == TaskType.MAP) ? info.minMaps : info.minReduces;
  }

  protected double weight(JobInProgress job, TaskType taskType) {
    JobInfo info = infos.get(job);
    if (info == null) return 0;
    return (taskType == TaskType.MAP ? info.mapWeight : info.reduceWeight);
  }

  protected double deficit(JobInProgress job, TaskType taskType) {
    JobInfo info = infos.get(job);
    if (info == null) return 0;
    return taskType == TaskType.MAP ? info.mapDeficit : info.reduceDeficit;
  }

  protected boolean isRunnable(JobInProgress job) {
    JobInfo info = infos.get(job);
    if (info == null) return false;
    return info.runnable;
  }

  @Override
  public synchronized Collection<JobInProgress> getJobs(String queueName) {
    Pool myJobPool = poolMgr.getPool(queueName);
    return myJobPool.getJobs();
  }
  
  /////////////////////////////////////////////
  // add by DC 
  /////////////////////////////////////////////
  private int DEFAULT_RECLAIM_TIME_LIMIT = ((JobTracker)taskTrackerManager).getReclaimInterval(); 
  /** 
   * For keeping track of reclaimed capacity. 
   * Whenever slots need to be reclaimed, we create one of these objects. 
   * As the queue gets slots, the amount to reclaim gets decremented. if 
   * we haven't reclaimed enough within a certain time, we need to kill 
   * tasks. This object 'expires' either if all resources are reclaimed
   * before the deadline, or the deadline passes . 
   */
  private static class ReclaimedResource {
    
	// which pool need reclaim
    public Pool pool;
    // we also keep track of when to kill tasks, im millisecs. 
    public long whenToKill;
    // task type: map or reduce
    public TaskType taskType;
    // is the resource occupied by job from the same pool
    public boolean innerPool = false;
    
    public ReclaimedResource(Pool pool, long whenToKill, 
    						TaskType taskType, boolean innerPool) {
      this.pool = pool;
      this.whenToKill = whenToKill;
      this.taskType = taskType;
      this.innerPool = innerPool;
    }
    
    public int hashCode() {
    	return pool.getName().hashCode() + (innerPool ? 1 : 0);
	}

	public boolean equals(Object obj) {
		if (obj instanceof ReclaimedResource) {
			if (((ReclaimedResource) obj).pool.getName().equals(pool.getName())
					&& ((ReclaimedResource) obj).innerPool == innerPool
					&& ((ReclaimedResource) obj).taskType == taskType)
				return true;
		}
		return false;
	}
  }
  
  /**
   * the list of resources to reclaim. This list is always sorted so that
   * resources that need to be reclaimed sooner occur earlier in the list.
   */
  private HashSet<ReclaimedResource> reclaimList = new HashSet<ReclaimedResource>();
  
  private void printPoolAndJobInfo(TaskType type) {
	  Logger.debug("\n**********Print All Info**********");
	  for(Pool pool: poolMgr.getPools()) {
		  int poolTaskSum = 0;
		  int poolRunnableSum = 0;
		  for(JobInProgress job : pool.getJobs()) {
				poolTaskSum += runningTasks(job, type);
				poolRunnableSum += runnableTasks(job, type);
			}
		  Logger.debug("\nPool Name:" + pool.getName() + "\n"
				  				+ "Pool Alloct:" + poolMgr.getAllocation(pool.getName(), type) + "\n"
				  				+ "Running Task:" + poolTaskSum + "\n"
				  				+ "Runnable Task:" + poolRunnableSum + "\n");
		  Logger.debug("\nJobID\tJob Type\tRunning Task\tNeeded Task\tFinished Task\tFairShare");
		  for(JobInProgress job : pool.getJobs()) {
			  Logger.debug("\n"+job.getJobID() + "\t"
					  				+ job.getJobConf().get("mapred.job.type", "experimental") + "\t"
					  				+ runningTasks(job, type) + "\t"
					  				+ neededTasks(job, type) + "\t"
					  				+ (type == TaskType.MAP ? infos.get(job).mapFairShare : infos.get(job).reduceFairShare) );
			}
	  }
	  Logger.debug("\n**********************************");
  }
  
  public synchronized void reclaimCapacity() throws IOException {
	if (!initialized) // Don't try to kill tasks if we haven't yet started up
		return;

	DEFAULT_RECLAIM_TIME_LIMIT = ((JobTracker)taskTrackerManager).getReclaimInterval(); 
	
	long currentTime = clock.getTime();
	// Scan to see whether any job needs to kill a map, then a reduce
	TaskType[] types = new TaskType[] { TaskType.MAP, TaskType.REDUCE };
	for (TaskType taskType : types) {
		Logger.debug("\n======================= " + new java.util.Date() + " Start Reclaim, Type : " + taskType + " =======================");
		printPoolAndJobInfo(taskType);
		boolean busy = false;
		for (Pool pool: poolMgr.getPools()) {
			// check the pool's total allocation 
			int deficitAlloc = poolDeficitAlloc(pool, taskType);
			Logger.debug("\nChecking Pool:" + pool.getName() + " DeficitAlloc:" + deficitAlloc);
			if(deficitAlloc > 0 && pool.getJobs().size() > 0) {
				// if a pool has deficit and the pool has a job and 
				// the job need tasks, then create ReclaimedResource request
				for(JobInProgress job : pool.getJobs()) {
					if (neededTasks(job, taskType) > 0) {
						Logger.debug("\nPool:" + pool.getName() + " Job:" + job.getJobID() + " neededTasks:" + neededTasks(job, taskType) + " Cause Inter Reclaim.");
						reclaimList.add(new ReclaimedResource(pool, 
								currentTime + DEFAULT_RECLAIM_TIME_LIMIT, 
								taskType, false));
						break;
					}
				}
			}else if (deficitAlloc < 0 && ((JobTracker)taskTrackerManager).isClusterBusy(taskType==TaskType.MAP ? "map":"reduce")) {
				// if at least one pool use more slots than it's 
				// capacity slots, then we say that the cluster is busy
				Logger.debug("\nCluster is busy and Pool:" + pool.getName() + " Make busy is true.");
				busy = true;
			}
			
			// check inner pool jobs
			if(needLocalReclaim(pool, taskType)) {
				Logger.debug("\nPool:" + pool.getName() + " Cause Inner Reclaim.");
				reclaimList.add(new ReclaimedResource(pool, 
						currentTime + DEFAULT_RECLAIM_TIME_LIMIT, 
						taskType, true));
			}
		}
        
		if (!busy) {
			continue;
		}
		
		// check the reclaimList, get the tasks to be killed
		int interPooltaskToKill = 0;
		List<Pool> poolNeedLocalReclaim = new ArrayList<Pool>();		
		Iterator<ReclaimedResource> it = reclaimList.iterator();
		while(it.hasNext()) {
			ReclaimedResource reclaimres = it.next();
			if(reclaimres.whenToKill < currentTime
					&& reclaimres.taskType == taskType) {
				if(reclaimres.innerPool == false) {
					int deficitAlloc = poolDeficitAlloc(reclaimres.pool, taskType);
					Logger.debug("\nPool in reclaim list:" + reclaimres.pool.getName() + " deficitAlloc:" + deficitAlloc);
					if(deficitAlloc > 0) {
						interPooltaskToKill += deficitAlloc;
					}
				} else {
					poolNeedLocalReclaim.add(reclaimres.pool);
				}
				it.remove();
			}
		}
		
		Logger.debug("\ninterPooltaskToKill:" + interPooltaskToKill + " poolNeedLocalReclaim size:" + poolNeedLocalReclaim.size() + " busy:" + busy);
		
		if((interPooltaskToKill == 0 && poolNeedLocalReclaim.isEmpty())){
			continue;
		}
		
		Logger.debug("\n----figure out candidate to kill. Step One----");
		
		// candidate jobs that has tasks need to be killed
		Map<JobInProgress, Integer> candidates
				= new HashMap<JobInProgress, Integer>();
		
		// inter-pool
		// Figure out the jobs that run too many of this type of tasks		
		double totalOverCap = 0;
		for (JobInProgress job : infos.keySet()) {
			double overCap = needBeReclaimed(job, taskType);
			Logger.debug("\nJob:" + job.getJobID() + " overCap:" + overCap);
			if (overCap > 0 && job.getStatus().getRunState() == JobStatus.RUNNING && isUseOtherSlots(job)) {
				totalOverCap += overCap;
				Logger.debug("\nJob:" + job.getJobID() + " was put into candidates.");
				candidates.put(job, (int)overCap);
			}
		}
		
		Logger.debug("\nCandidates size is:" + candidates.size());
		
		if(candidates.size() == 0) 
			continue;
		
		Logger.debug("\n----figure out candidate to kill. Step Two----");
		
		Logger.debug("\nTotalOverCap:" + totalOverCap + " interPooltaskToKill:" + interPooltaskToKill);
		
		// kill part of he over capacity tasks
		if(totalOverCap > interPooltaskToKill) {
			for(JobInProgress job : candidates.keySet()) {
				Logger.debug("\nJob:" + job.getJobID() + " kill part of this job.");
				candidates.put(job, (int)Math.round(
					needBeReclaimed(job, taskType) * (double) interPooltaskToKill / totalOverCap));
			}
		} // else totalOverCap < taskToKill, only kill the over capacity tasks
		
		Logger.debug("\n----figure out candidate to kill. Step Three----");
		
		// inner-pool
		for(Pool pool : poolNeedLocalReclaim) {
			// check again
			if(needLocalReclaim(pool, taskType)) {
				for(JobInProgress job : pool.getJobs()){
					double profit = needBeReclaimed(job, taskType);
					if(!isProductJob(job) && profit > 0) {
						Logger.debug("\nInner kill: Pool:" + pool.getName() + " Job:" + job.getJobID() + " profit:" + profit);
						candidates.put(job, (int)profit);
					}
				}
			}
		}
		
		Logger.debug("\nAll candidates:");
		for (Map.Entry<JobInProgress, Integer> entry : candidates
				.entrySet()) {
			JobInProgress job = entry.getKey();
			int tasksToKill = entry.getValue();
			Logger.debug("\nNeed to kill job:" + job.getJobID() + " taskToKill is:" + tasksToKill);
		}
		killTasks(candidates, taskType);
		Logger.debug("\n======================= " + new java.util.Date() + " End Reclaim, Type : " + taskType + " =======================");
	}
  }
  
  /*
   * return the pool deficit slots
   * if the value returned > 0 say the pool's slots been taken by ohters
   * if the value returned < 0 say the pool taken other's slots
   * when the total runnable task < pool allocations return
   * the real needed slots
   */
  private int poolDeficitAlloc(Pool pool, TaskType taskType) {
	int poolTaskSum = 0;
	int poolRunnableSum = 0;
	for(JobInProgress job : pool.getJobs()) {
		poolTaskSum += runningTasks(job, taskType);
		poolRunnableSum += runnableTasks(job, taskType);
	}
	int poolAlloc = poolMgr.getAllocation(pool.getName(), taskType);	
	return Math.min(poolAlloc, poolRunnableSum) - poolTaskSum;
  }

  private double needBeReclaimed(JobInProgress job, TaskType type) {
	JobInfo info = infos.get(job);
	if(type == TaskType.MAP) {
		return (info.runningMaps - info.mapFairShare);
	} else {
		return (info.runningReduces - info.reduceFairShare);
	}
  }  

	// return the TaskAttemptID of the running task, if any, that has made
	// the least progress.
	private TaskAttemptID getRunningTaskWithLeastProgress(TaskInProgress tip) {
		double leastProgress = 1;
		TaskAttemptID tID = null;
		for (Iterator<TaskAttemptID> it = tip.getActiveTasks().keySet()
				.iterator(); it.hasNext();) {
			TaskAttemptID taskid = it.next();
			TaskStatus status = tip.getTaskStatus(taskid);
			if (status != null && status.getRunState() == TaskStatus.State.RUNNING) {
				if (status.getProgress() < leastProgress) {
					leastProgress = status.getProgress();
					tID = taskid;
				}
			}
		}
		return tID;
	}
	
	private int killTasks(Map<JobInProgress, Integer> candidates,
			TaskType taskType) {
		
		/*
		 * We'd like to kill tasks that ran the last, or that have made the
		 * least progress.
		 * Ideally, each job would have a list of tasks, sorted by start 
		 * time or progress. That's a lot of state to keep, however. 
		 * For now, we do something a little different. We first try and kill
		 * non-local tasks, as these can be run anywhere. For each TIP, we 
		 * kill the task that has made the least progress, if the TIP has
		 * more than one active task. 
		 * We then look at tasks in runningMapCache.
		 */
		int tasksAllKilled = 0;
		/* 
		 * For non-local running maps, we 'cheat' a bit. We know that the set
		 * of non-local running maps has an insertion order such that tasks 
		 * that ran last are at the end. So we iterate through the set in 
		 * reverse. This is OK because even if the implementation changes, 
		 * we're still using generic set iteration and are no worse of.
		 */
		if (taskType == TaskType.MAP) {
			for (Map.Entry<JobInProgress, Integer> entry : candidates
					.entrySet()) {
				int tasksKilled = 0;
				JobInProgress job = entry.getKey();
				int tasksToKill = entry.getValue();
				TaskInProgress[] tips;
				if(job.getJobConf().get("mapred.map.tasks.speculative.execution").equals("true")){
					 tips = job.getNonLocalRunningMaps().toArray(new TaskInProgress[0]);
				}else{
					tips = job.getMapTasks();
				}
				for (int i = tips.length - 1; i >= 0; i--) {
					// pick the tast attempt that has progressed least
					TaskAttemptID tid = getRunningTaskWithLeastProgress(tips[i]);
					if (null != tid) {
						if (tips[i].killTask(tid, false)) {
							tasksAllKilled++;
							if (++tasksKilled >= tasksToKill) {
								break;
							}
						}
					}
				}
				if(tasksKilled < tasksToKill){
					//now look at other running tasks
					for (Set<TaskInProgress> s : job.getRunningMapCache().values()) {
						boolean isEnough = false;
						for (TaskInProgress tip : s) {
							TaskAttemptID tid = getRunningTaskWithLeastProgress(tip);
							if (null != tid) {
								if (tip.killTask(tid, false)) {
									tasksAllKilled++;
									if (++tasksKilled >= tasksToKill) {
										isEnough = true;
										break;
									}
								}
							}
						}
						if(isEnough){
							break;
						}
					}
				}
				LOG.warn("--------------------- killed "+ tasksKilled +" map task  -----------------");
			}
		} else {
			/* 
			 * For reduces, we 'cheat' a bit. We know that the set
			 * of running reduces has an insertion order such that tasks 
			 * that ran last are at the end. So we iterate through the set in 
			 * reverse. This is OK because even if the implementation changes, 
			 * we're still using generic set iteration and are no worse of.
			 */
			for (Map.Entry<JobInProgress, Integer> entry : candidates
					.entrySet()) {
				int tasksKilled = 0;
				JobInProgress job = entry.getKey();
				int tasksToKill = entry.getValue();
				TaskInProgress[] tips = 
			        job.getRunningReduces().toArray(new TaskInProgress[0]);
				for (int i=tips.length-1; i>=0; i--) {
					TaskAttemptID tid = getRunningTaskWithLeastProgress(tips[i]);
					if (null != tid) {
						if (tips[i].killTask(tid, false)) {
							tasksAllKilled++;
							if (++tasksKilled >= tasksToKill) {
								break;
							}
						}
					}
				}
				LOG.warn("--------------------- killed "+ tasksKilled +" reduce task  -----------------");
			}
		}
		return tasksAllKilled;
	}
	
	public static  boolean isProductJob(JobInProgress job){
		boolean isProduct = job.getJobConf().get("mapred.job.type", "experimental").equals("production");
		return isProduct;
	}
	
	/*
     * check if this pool need reclaim locally 
     * when there are both production and non-production
     * jobs in this pool and non-production jobs have taken
     * more slots than they deserved, a reclaim is needed 
     */
    private boolean needLocalReclaim(Pool pool, TaskType type) {
	  boolean hasProduct = false;
	  boolean hasNonProduct = false;
	  double productProfit = 0;
	  double nonProductProfit = 0;
	  for(JobInProgress job : pool.getJobs()){
		  if(FairScheduler.isProductJob(job)) {
			  hasProduct = true;
			  productProfit += needBeReclaimed(job, type);
		  } else { 
			  hasNonProduct = true;
			  nonProductProfit += needBeReclaimed(job, type);
		  }
	  }
	  return hasProduct && hasNonProduct &&
	  			productProfit < 0 && nonProductProfit > 0;
    }
}
