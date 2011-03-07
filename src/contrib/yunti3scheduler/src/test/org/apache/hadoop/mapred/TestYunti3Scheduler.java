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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.JobClient.RawSplit;
import org.apache.hadoop.mapred.Yunti3Scheduler.JobInfo;
import org.apache.hadoop.net.Node;

public class TestYunti3Scheduler extends TestCase {
  final static String TEST_DIR = new File(System.getProperty("test.build.data",
  		"build/contrib/yunti3scheduler/test/data")).getAbsolutePath();
  final static String ALLOC_FILE = new File(TEST_DIR, 
  		"test-pools").getAbsolutePath();
  
  private static final String POOL_PROPERTY = "pool";
  private static final String JOB_LEVEL = "mapred.job.level";
  
  private static int jobCounter;
  private static int taskCounter;
  
  class FakeJobInProgress extends JobInProgress {
    
    private FakeTaskTrackerManager taskTrackerManager;
    private final String[][] mapInputLocations; // Array of hosts for each map
    
    public FakeJobInProgress(JobConf jobConf,
        FakeTaskTrackerManager taskTrackerManager,
        String[][] mapInputLocations, JobTracker jt) throws IOException {
      super(new JobID("test", ++jobCounter), jobConf);
      this.taskTrackerManager = taskTrackerManager;
      this.startTime = System.currentTimeMillis();
      this.mapInputLocations = mapInputLocations;
      this.status = new JobStatus();
      this.status.setRunState(JobStatus.PREP);
      this.nonLocalMaps = new LinkedList<TaskInProgress>();
      this.nonLocalRunningMaps = new LinkedHashSet<TaskInProgress>();
      this.runningMapCache = new IdentityHashMap<Node, Set<TaskInProgress>>();
      this.nonRunningReduces = new LinkedList<TaskInProgress>();   
      this.runningReduces = new LinkedHashSet<TaskInProgress>();
      this.jobtracker = jt;
      initTasks();
    }
    
    @Override
    public synchronized void initTasks() throws IOException {
   // initTasks is needed to create non-empty cleanup and setup TIP
      // arrays, otherwise calls such as job.getTaskInProgress will fail
      JobID jobId = getJobID();
      JobConf conf = getJobConf();
      numMapTasks = conf.getNumMapTasks();
      String jobFile = "";
      // create two cleanup tips, one map and one reduce.
      cleanup = new TaskInProgress[2];
      // cleanup map tip.
      cleanup[0] = new TaskInProgress(jobId, jobFile, null, 
              jobtracker, conf, this, numMapTasks);
      cleanup[0].setJobCleanupTask();
      // cleanup reduce tip.
      cleanup[1] = new TaskInProgress(jobId, jobFile, numMapTasks,
                         numReduceTasks, jobtracker, conf, this);
      cleanup[1].setJobCleanupTask();
      // create two setup tips, one map and one reduce.
      setup = new TaskInProgress[2];
      // setup map tip.
      setup[0] = new TaskInProgress(jobId, jobFile, null, 
              jobtracker, conf, this, numMapTasks + 1);
      setup[0].setJobSetupTask();
      // setup reduce tip.
      setup[1] = new TaskInProgress(jobId, jobFile, numMapTasks,
                         numReduceTasks + 1, jobtracker, conf, this);
      setup[1].setJobSetupTask();
      // create maps
      maps = new TaskInProgress[numMapTasks];
      // empty format
      for (int i = 0; i < numMapTasks; i++) {
        String[] inputLocations = null;
        if (mapInputLocations != null)
          inputLocations = mapInputLocations[i];
        maps[i] = new FakeTaskInProgress(getJobID(), i, 
            getJobConf(), this, inputLocations);
        if (mapInputLocations == null) // Job has no locality info
          nonLocalMaps.add(maps[i]);
      }
      // create reduces
      numReduceTasks = conf.getNumReduceTasks();
      reduces = new TaskInProgress[numReduceTasks];
      for (int i = 0; i < numReduceTasks; i++) {
        reduces[i] = new FakeTaskInProgress(getJobID(), i, getJobConf(), this);
      }
    }
    
    @Override
    public Task obtainNewMapTask(final TaskTrackerStatus tts, int clusterSize,
        int numUniqueHosts, int localityLevel) throws IOException {
      for (int map = 0; map < maps.length; map++) {
        FakeTaskInProgress tip = (FakeTaskInProgress) maps[map];
        if (!tip.isRunning() && !tip.isComplete() &&
            getLocalityLevel(tip, tts) < localityLevel) {
          TaskAttemptID attemptId = getTaskAttemptID(tip);
          Task task = new MapTask("", attemptId, 0, null, null) {
            @Override
            public String toString() {
              return String.format("%s on %s", getTaskID(), tts.getTrackerName());
            }
          };
          runningMapTasks++;
          tip.createTaskAttempt(task, tts.getTrackerName());
          nonLocalRunningMaps.add(tip);
          taskTrackerManager.startTask(tts.getTrackerName(), task, tip);
          return task;
        }
      }
      return null;
    }
    
    private TaskAttemptID getTaskAttemptID(TaskInProgress tip) {
      JobID jobId = getJobID();
      return new TaskAttemptID(jobId.getJtIdentifier(), jobId.getId(),
          tip.isMapTask(), tip.getIdWithinJob(), tip.nextTaskId++);
    }
    
    @Override
    public Task obtainNewReduceTask(final TaskTrackerStatus tts,
        int clusterSize, int ignored) throws IOException {
      for (int reduce = 0; reduce < reduces.length; reduce++) {
        FakeTaskInProgress tip = 
          (FakeTaskInProgress) reduces[reduce];
        if (!tip.isRunning() && !tip.isComplete()) {
          TaskAttemptID attemptId = getTaskAttemptID(tip);
          Task task = new ReduceTask("", attemptId, 0, maps.length) {
            @Override
            public String toString() {
              return String.format("%s on %s", getTaskID(), tts.getTrackerName());
            }
          };
          runningReduceTasks++;
          tip.createTaskAttempt(task, tts.getTrackerName());
          runningReduces.add(tip);
          taskTrackerManager.startTask(tts.getTrackerName(), task, tip);
          return task;
        }
      }
      return null;
    }
    
    public void mapTaskFinished(TaskInProgress tip) {
      runningMapTasks--;
      finishedMapTasks++;
      nonLocalRunningMaps.remove(tip);
    }
    
    public void reduceTaskFinished(TaskInProgress tip) {
      runningReduceTasks--;
      finishedReduceTasks++;
      runningReduces.remove(tip);
    }
    
    @Override
    int getLocalityLevel(TaskInProgress tip, TaskTrackerStatus tts) {
      FakeTaskInProgress ftip = (FakeTaskInProgress) tip;
      if (ftip.inputLocations != null) {
        // Check whether we're on the same host as an input split
        for (String location: ftip.inputLocations) {
          if (location.equals(tts.host)) {
            return 0;
          }
        }
        // Check whether we're on the same rack as an input split
        for (String location: ftip.inputLocations) {
          if (getRack(location).equals(getRack(tts.host))) {
            return 1;
          }
        }
        // Not on same rack or host
        return 2;
      } else {
        // Job has no locality info  
        return -1;
      }
    }
  }
  
  class FakeTaskInProgress extends TaskInProgress {
    private boolean isMap;
    private FakeJobInProgress fakeJob;
    private TreeMap<TaskAttemptID, String> activeTasks;
    private TaskStatus taskStatus;
    private boolean isComplete = false;
    private String[] inputLocations;
    
    // Constructor for map
    FakeTaskInProgress(JobID jId, int id, JobConf jobConf,
        FakeJobInProgress job, String[] inputLocations) {
      super(jId, "", new RawSplit(), jt, jobConf, job, id);
      this.isMap = true;
      this.fakeJob = job;
      this.inputLocations = inputLocations;
      activeTasks = new TreeMap<TaskAttemptID, String>();
      taskStatus = TaskStatus.createTaskStatus(isMap);
      taskStatus.setRunState(TaskStatus.State.UNASSIGNED);
    }

    // Constructor for reduce
    FakeTaskInProgress(JobID jId, int id, JobConf jobConf,
        FakeJobInProgress job) {
      super(jId, "", jobConf.getNumMapTasks(), id, jt, jobConf, job);
      this.isMap = false;
      this.fakeJob = job;
      activeTasks = new TreeMap<TaskAttemptID, String>();
      taskStatus = TaskStatus.createTaskStatus(isMap);
      taskStatus.setRunState(TaskStatus.State.UNASSIGNED);
    }
    
    private void createTaskAttempt(Task task, String taskTracker) {
      activeTasks.put(task.getTaskID(), taskTracker);
      taskStatus = TaskStatus.createTaskStatus(isMap, task.getTaskID(),
          0.5f, TaskStatus.State.RUNNING, "", "", "", 
          TaskStatus.Phase.STARTING, new Counters());
      taskStatus.setStartTime(clock.getTime());
    }
    
    @Override
    TreeMap<TaskAttemptID, String> getActiveTasks() {
      return activeTasks;
    }
    
    public synchronized boolean isComplete() {
      return isComplete;
    }
    
    public boolean isRunning() {
      return activeTasks.size() > 0;
    }
    
    @Override
    public TaskStatus getTaskStatus(TaskAttemptID taskid) {
      return taskStatus;
    }
    
    void killAttempt() {
      if (isMap) {
        fakeJob.mapTaskFinished(this);
      }
      else {
        fakeJob.reduceTaskFinished(this);
      }
      activeTasks.clear();
      taskStatus.setRunState(TaskStatus.State.UNASSIGNED);
    }
    
    void finishAttempt() {
      isComplete = true;
      if (isMap) {
        fakeJob.mapTaskFinished(this);
      }
      else {
        fakeJob.reduceTaskFinished(this);
      }
      activeTasks.clear();
      taskStatus.setRunState(TaskStatus.State.UNASSIGNED);
    }
  }
  
  static class FakeTaskTrackerManager implements TaskTrackerManager {
    int maps = 0;
    int reduces = 0;
    int maxMapTasksPerTracker = 2;
    int maxReduceTasksPerTracker = 2;
    long ttExpiryInterval = 10 * 60 * 1000L; // default interval
    List<JobInProgressListener> listeners =
      new ArrayList<JobInProgressListener>();
    Map<JobID, JobInProgress> jobs = new HashMap<JobID, JobInProgress>();
    
    private Map<String, TaskTrackerStatus> trackers =
      new HashMap<String, TaskTrackerStatus>();
    private Map<String, TaskStatus> statuses = 
      new HashMap<String, TaskStatus>();
    private Map<String, FakeTaskInProgress> tips = 
      new HashMap<String, FakeTaskInProgress>();
    private Map<String, TaskTrackerStatus> trackerForTip =
      new HashMap<String, TaskTrackerStatus>();
    
    public FakeTaskTrackerManager(int numRacks, int numTrackersPerRack) {
      int nextTrackerId = 1;
      for (int rack = 1; rack <= numRacks; rack++) {
        for (int node = 1; node <= numTrackersPerRack; node++) {
          int id = nextTrackerId++;
          String host = "rack" + rack + ".node" + node;
          System.out.println("Creating TaskTracker tt" + id + " on " + host);
          TaskTrackerStatus tts = new TaskTrackerStatus("tt" + id, host, 0,
              new ArrayList<TaskStatus>(), 0,
              maxMapTasksPerTracker, maxReduceTasksPerTracker);
          trackers.put("tt" + id, tts);
        }
      }
    }
    
    @Override
    public ClusterStatus getClusterStatus() {
      int numTrackers = trackers.size();

      return new ClusterStatus(numTrackers, maps, reduces,
          numTrackers * maxMapTasksPerTracker,
          numTrackers * maxReduceTasksPerTracker,
          JobTracker.State.RUNNING);
    }

    @Override
    public QueueManager getQueueManager() {
      return null;
    }
    
    @Override
    public int getNumberOfUniqueHosts() {
      return trackers.size();
    }

    @Override
    public Collection<TaskTrackerStatus> taskTrackers() {
      List<TaskTrackerStatus> statuses = new ArrayList<TaskTrackerStatus>();
      for (TaskTrackerStatus tts : trackers.values()) {
        statuses.add(tts);
      }
      return statuses;
    }


    @Override
    public void addJobInProgressListener(JobInProgressListener listener) {
      listeners.add(listener);
    }

    @Override
    public void removeJobInProgressListener(JobInProgressListener listener) {
      listeners.remove(listener);
    }
    
    @Override
    public int getNextHeartbeatInterval() {
      return MRConstants.HEARTBEAT_INTERVAL_MIN;
    }

    public void initJob (JobInProgress job) {
      // do nothing
    }
    
    public void failJob (JobInProgress job) {
      // do nothing
    }
    
    // Test methods
    
    public void submitJob(JobInProgress job) throws IOException {
      jobs.put(job.getJobID(), job);
      for (JobInProgressListener listener : listeners) {
        listener.jobAdded(job);
      }
    }
    
    public TaskTrackerStatus getTaskTracker(String trackerID) {
      return trackers.get(trackerID);
    }
    
    public void startTask(String trackerName, Task t, FakeTaskInProgress tip) {
      final boolean isMap = t.isMapTask();
      if (isMap) {
        maps++;
      } else {
        reduces++;
      }
      String attemptId = t.getTaskID().toString();
      TaskStatus status = tip.getTaskStatus(t.getTaskID());
      TaskTrackerStatus trackerStatus = trackers.get(trackerName);
      tips.put(attemptId, tip);
      statuses.put(attemptId, status);
      trackerForTip.put(attemptId, trackerStatus);
      status.setRunState(TaskStatus.State.RUNNING);
      trackerStatus.getTaskReports().add(status);
    }
    
    public void finishTask(String taskTrackerName, String attemptId) {
      FakeTaskInProgress tip = tips.get(attemptId);
      if (tip.isMapTask()) {
        maps--;
      } else {
        reduces--;
      }
      tip.finishAttempt();
      TaskStatus status = statuses.get(attemptId);
      trackers.get(taskTrackerName).getTaskReports().remove(status);
    }
  }
  
  protected class FakeClock extends Yunti3Scheduler.Clock {
    private long time = 0;
    
    public void advance(long millis) {
      time += millis;
    }

    @Override
    long getTime() {
      return time;
    }
  }
  
  protected JobConf conf;
  protected Yunti3Scheduler scheduler;
  private FakeTaskTrackerManager taskTrackerManager;
  private JobTracker jt = new JobTracker();
  private FakeClock clock;

  @Override
  protected void setUp() throws Exception {
    jobCounter = 0;
    taskCounter = 0;
    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    // Create an empty pools file (so we can add/remove pools later)
    FileWriter fileWriter = new FileWriter(ALLOC_FILE);
    fileWriter.write("<?xml version=\"1.0\"?>\n");
    fileWriter.write("<allocations>"); 
    // Give default pool a minimum of 4 map, 4 reduces
    fileWriter.write("<pool name=\"default\">");
    fileWriter.write("<minMaps>4</minMaps>");
    fileWriter.write("<minReduces>4</minReduces>");
    fileWriter.write("</pool>");
    fileWriter.write("</allocations>"); 
    fileWriter.close();
    setUpCluster(1, 2, false);
  }
  
  private void setUpCluster(int numRacks, int numNodesPerRack,
      boolean assignMultiple) {
    conf = new JobConf();
    conf.set("mapred.yunti3scheduler.allocation.file", ALLOC_FILE);
    conf.set("mapred.yunti3scheduler.poolnameproperty", POOL_PROPERTY);
    conf.setBoolean("mapred.yunti3scheduler.assignmultiple", assignMultiple);
    if (assignMultiple) {
      conf.setInt("mapred.yunti3scheduler.assignmultiple.maps", -1);
      conf.setInt("mapred.yunti3scheduler.assignmultiple.reduces", -1);
    }   
    // Manually set locality delay because we aren't using a JobTracker so
    // we can't auto-compute it from the heartbeat interval.
    conf.setLong("mapred.yunti3scheduler.locality.delay", 10000);
    taskTrackerManager = new FakeTaskTrackerManager(numRacks, numNodesPerRack);
    clock = new FakeClock();
    scheduler = new Yunti3Scheduler(clock, true);
    scheduler.waitForMapsBeforeLaunchingReduces = false;
    scheduler.setConf(conf);
    scheduler.setTaskTrackerManager(taskTrackerManager);
    scheduler.start();
  }
  
  public String getRack(String hostname) {
    // Host names are of the form rackN.nodeM, so split at the dot.
    return hostname.split("\\.")[0];
  }
  
  @Override
  protected void tearDown() throws Exception {
    if (scheduler != null) {
      scheduler.terminate();
    }
  }
  
  private JobInProgress submitJob(int state, int maps, int reduces)
      throws IOException {
    return submitJob(state, maps, reduces, null, null);
  }

  private JobInProgress submitJob(int state, int maps, int reduces, String pool)
      throws IOException {
    return submitJob(state, maps, reduces, pool, null);
  }
  
  private JobInProgress submitJob(int state, int maps, int reduces,
      String pool, int jobLevel) throws IOException {
    return submitJob(state, maps, reduces, pool, null, jobLevel);
  }
  
  private JobInProgress submitJob(int state, int maps, int reduces,
      String pool, String[][] mapInputLocations) throws IOException {
    return submitJob(state, maps, reduces, pool, mapInputLocations, 0);
  }

  private JobInProgress submitJob(int state, int maps, int reduces,
      String pool, String[][] mapInputLocations, int jobLevel) throws IOException {
    JobConf jobConf = new JobConf(conf);
    jobConf.setNumMapTasks(maps);
    jobConf.setNumReduceTasks(reduces);
    if (pool != null)
      jobConf.set(POOL_PROPERTY, pool);
    jobConf.setInt(JOB_LEVEL, jobLevel);
    JobInProgress job = new FakeJobInProgress(jobConf, taskTrackerManager,
        mapInputLocations, jt);
    job.getStatus().setRunState(state);
    taskTrackerManager.submitJob(job);
    job.startTime = clock.time;
    return job;
  }

  protected void submitJobs(int number, int state, int maps, int reduces)
      throws IOException {
    for (int i = 0; i < number; i++) {
      submitJob(state, maps, reduces);
    }
  }

  public void testAllocationFileParsing() throws Exception {
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>"); 
    // Give pool A a minimum of 1 map, 2 reduces
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>1</minMaps>");
    out.println("<minReduces>2</minReduces>");    
    out.println("</pool>");
    // Give pool B a minimum of 2 maps, 1 reduce
    out.println("<pool name=\"poolB\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>1</minReduces>");    
    out.println("</pool>");
    // Give pool C min maps but no min reduces
    out.println("<pool name=\"poolC\">");
    out.println("<minMaps>2</minMaps>");    
    out.println("</pool>");
    // Give pool D a limit of 3 running jobs
    out.println("<pool name=\"poolD\">");
    out.println("<maxRunningJobs>3</maxRunningJobs>");    
    out.println("</pool>");
    // Set default limit of jobs per user to 5
    out.println("<userMaxJobsDefault>5</userMaxJobsDefault>");
    // Give user1 a limit of 10 jobs
    out.println("<user name=\"user1\">");
    out.println("<maxRunningJobs>10</maxRunningJobs>");
    out.println("</user>");
    out.println("</allocations>"); 
    out.close();
    
    YTPoolManager poolManager = scheduler.getPoolManager();
    poolManager.reloadAllocs();
    
    assertEquals(4, poolManager.getPools().size()); // 4 in file
    assertEquals(1, poolManager.getAllocation("poolA", YTTaskType.MAP));
    assertEquals(2, poolManager.getAllocation("poolA", YTTaskType.REDUCE));
    assertEquals(2, poolManager.getAllocation("poolB", YTTaskType.MAP));
    assertEquals(1, poolManager.getAllocation("poolB", YTTaskType.REDUCE));
    assertEquals(2, poolManager.getAllocation("poolC", YTTaskType.MAP));
    assertEquals(0, poolManager.getAllocation("poolC", YTTaskType.REDUCE));
    assertEquals(0, poolManager.getAllocation("poolD", YTTaskType.MAP));
    assertEquals(0, poolManager.getAllocation("poolD", YTTaskType.REDUCE));
    assertEquals(32, poolManager.getPoolMaxJobs("poolA")); // 32 is default
    assertEquals(3, poolManager.getPoolMaxJobs("poolD"));
    assertEquals(10, poolManager.getUserMaxJobs("user1"));
    assertEquals(5, poolManager.getUserMaxJobs("user2"));
  }
  
  public void testTaskNotAssignedWhenNoJobsArePresent() throws IOException {
    assertNull(scheduler.assignTasks(tracker("tt1")));
  }

  public void testNonRunningJobsAreIgnored() throws IOException {
    submitJobs(1, JobStatus.PREP, 10, 10);
    submitJobs(1, JobStatus.SUCCEEDED, 10, 10);
    submitJobs(1, JobStatus.FAILED, 10, 10);
    submitJobs(1, JobStatus.KILLED, 10, 10);
    assertNull(scheduler.assignTasks(tracker("tt1")));
    advanceTime(100); // Check that we still don't assign jobs after an update
    assertNull(scheduler.assignTasks(tracker("tt1")));
  }

  /**
   * This test contains two jobs with fewer required tasks than there are slots.
   * We check that all tasks are assigned, but job 1 gets them first because it
   * was submitted earlier.
   */
  public void testSmallJobs() throws IOException {
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 2, 1);
    advanceTime(1000);
    JobInfo info1 = scheduler.getJobInfo(job1);
    
    // Check scheduler variables
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(2,    info1.neededMaps);
    assertEquals(1,    info1.neededReduces);    
    assertEquals(2.0,  info1.mapFairShare);
    assertEquals(1.0,  info1.reduceFairShare);
    
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 1, 2);
    advanceTime(1000);
    JobInfo info2 = scheduler.getJobInfo(job2);
    
    // Check scheduler variables; the fair shares should now have been allocated
    // equally between j1 and j2, but j1 should have (4 slots)*(100 ms) deficit
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(2,    info1.neededMaps);
    assertEquals(1,    info1.neededReduces);    
    assertEquals(2.0,  info1.mapFairShare);
    assertEquals(1.0,  info1.reduceFairShare);
    assertEquals(0,    info2.runningMaps);
    assertEquals(0,    info2.runningReduces);
    assertEquals(1,    info2.neededMaps);
    assertEquals(2,    info2.neededReduces);    
    assertEquals(1.0,  info2.mapFairShare);
    assertEquals(2.0,  info2.reduceFairShare);
    
    // Assign tasks and check that jobs alternate in filling slots
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
    assertNull(scheduler.assignTasks(tracker("tt2")));
    
    // Check that the scheduler has started counting the tasks as running
    // as soon as it launched them.
    assertEquals(2, info1.runningMaps);
    assertEquals(1, info1.runningReduces);
    assertEquals(0, info1.neededMaps);
    assertEquals(0, info1.neededReduces);
    assertEquals(1, info2.runningMaps);
    assertEquals(2, info2.runningReduces);
    assertEquals(0, info2.neededMaps);
    assertEquals(0, info2.neededReduces);
  }
  
  /**
   * This test begins by submitting two jobs with 10 maps and reduces each.
   * Job2 is submitted 1000ms after job1, to make sure job1 gets slot first. 
   * After this, we assign tasks to all slots, which should all be
   * from job 1. These run for 200ms, at which point job 2 now has a deficit
   * of 400 while job 1 is down to a deficit of 0. We then finish all tasks and
   * assign new ones, which should all be from job 2. These run for 50 ms,
   * which is not enough time for job 2 to make up its deficit (it only makes up
   * 100 ms of deficit). Finally we assign a new round of tasks, which should
   * all be from job 2 again.
   */
  public void testLargeJobs() throws IOException {
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    advanceTime(1000);
    JobInfo info1 = scheduler.getJobInfo(job1);
    
    // Check scheduler variables
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(10,   info1.neededMaps);
    assertEquals(10,   info1.neededReduces);    
    assertEquals(4.0,  info1.mapFairShare);
    assertEquals(4.0,  info1.reduceFairShare);
    

    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    advanceTime(1000);
    JobInfo info2 = scheduler.getJobInfo(job2);
    
    // Check scheduler variables; the fair shares should now have been allocated
    // equally between j1 and j2, but j1 should have (1 slots)*(100 ms) +  deficit
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(10,   info1.neededMaps);
    assertEquals(10,   info1.neededReduces);
    assertEquals(2.0,  info1.mapFairShare);
    assertEquals(2.0,  info1.reduceFairShare);
    assertEquals(0,    info2.runningMaps);
    assertEquals(0,    info2.runningReduces);
    assertEquals(10,   info2.neededMaps);
    assertEquals(10,   info2.neededReduces);
    assertEquals(2.0,  info2.mapFairShare);
    assertEquals(2.0,  info2.reduceFairShare);
    
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
    
    // Check that the scheduler has started counting the tasks as running
    // as soon as it launched them.
    assertEquals(2,  info1.runningMaps);
    assertEquals(2,  info1.runningReduces);
    assertEquals(8,  info1.neededMaps);
    assertEquals(8,  info1.neededReduces);
    assertEquals(2,  info2.runningMaps);
    assertEquals(2,  info2.runningReduces);
    assertEquals(8, info2.neededMaps);
    assertEquals(8, info2.neededReduces);
    
    // Finish up the tasks and advance time again. Note that we must finish
    // the task since FakeJobInProgress does not properly maintain running
    // tasks, so the scheduler will always get an empty task list from
    // the JobInProgress's getMapTasks/getReduceTasks and think they finished.
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000000_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_r_000000_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000001_0");
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_r_000001_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_m_000000_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_r_000000_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_m_000001_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_r_000001_0");
    advanceTime(1000);
    assertEquals(0,   info1.runningMaps);
    assertEquals(0,   info1.runningReduces);
    assertEquals(0,   info2.runningMaps);
    assertEquals(0,   info2.runningReduces);

    // Check that tasks are filled alternately by the jobs
    checkAssignment("tt1", "attempt_test_0001_m_000002_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000002_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000003_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000003_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000002_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000003_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000003_0 on tt2");
  }
  

  /**
   * We submit two jobs such that one has 2x the priority of the other, wait
   * for 100 ms, and check that the weights/deficits are okay and that the
   * tasks all go to the high-priority job.
   */
  public void testJobsWithPriorities() throws IOException {
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info1 = scheduler.getJobInfo(job1);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info2 = scheduler.getJobInfo(job2);
    job2.setPriority(JobPriority.HIGH);
    advanceTime(1000);
    
    // Check scheduler variables 
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(10,   info1.neededMaps);
    assertEquals(10,   info1.neededReduces);
    assertEquals(1.0, info1.mapWeight, 0.1);
    assertEquals(1.0, info1.reduceWeight, 0.1);
    assertEquals(0,    info2.runningMaps);
    assertEquals(0,    info2.runningReduces);
    assertEquals(10,   info2.neededMaps);
    assertEquals(10,   info2.neededReduces);
    assertEquals(2.0, info2.mapWeight, 0.1);
    assertEquals(2.0, info2.reduceWeight, 0.1);
    
    advanceTime(1000);
    
    // Assign tasks and check that all slots are filled with j1, then j2
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
  }
  
  /**
   * This test starts by submitting three large jobs:
   * - job1 in the default pool, at time 0
   * - job2 in poolA, with an allocation of 1 map / 2 reduces, at time 200
   * - job3 in poolB, with an allocation of 2 maps / 1 reduce, at time 200
   * 
   * After this, we sleep 100ms, until time 300. At this point, job1 has the
   * highest map deficit, job3 the second, and job2 the third. This is because
   * job3 has more maps in its min share than job2, but job1 has been around
   * a long time at the beginning. The reduce deficits are similar, except job2
   * comes before job3 because it had a higher reduce minimum share.
   * 
   * Finally, assign tasks to all slots. The maps should be assigned in the
   * order job3, job2, job1 because 3 and 2 both have guaranteed slots and 3
   * has a higher deficit. The reduces should be assigned as job2, job3, job1.
   */
  public void testLargeJobsWithPools() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Define the default pool with minimum of 4 map, 4 reduces
    out.println("<pool name=\"default\">");
    out.println("<minMaps>4</minMaps>");
    out.println("<minReduces>4</minReduces>");
    out.println("</pool>");
    // Give pool A a minimum of 1 map, 2 reduces
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>1</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("</pool>");
    // Give pool B a minimum of 2 maps, 1 reduce
    out.println("<pool name=\"poolB\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>1</minReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    advanceTime(1000);
    JobInfo info1 = scheduler.getJobInfo(job1);
    
    // Check scheduler variables
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(10,   info1.neededMaps);
    assertEquals(10,   info1.neededReduces);

    assertEquals(4.0,  info1.mapFairShare);
    assertEquals(4.0,  info1.reduceFairShare);
    

    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    advanceTime(1000);
    JobInfo info2 = scheduler.getJobInfo(job2);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "poolB");
    advanceTime(1000);
    JobInfo info3 = scheduler.getJobInfo(job3);
    
    // Check that minimum and fair shares have been allocated
    assertEquals(4.0,  info1.mapFairShare);
    assertEquals(4.0,  info1.reduceFairShare);
    assertEquals(1.0,  info2.mapFairShare);
    assertEquals(2.0,  info2.reduceFairShare);
    assertEquals(2.0,  info3.mapFairShare);
    assertEquals(1.0,  info3.reduceFairShare);

    // Assign tasks and check that slots are first given to needy jobs
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0003_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0003_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
  }

  /**
   * This test starts by submitting three large jobs:
   * - job1 in the default pool, at time 0
   * - job2 in poolA, with an allocation of 2 maps / 2 reduces, at time 200
   * - job3 in poolA, with an allocation of 2 maps / 2 reduces, at time 300
   * 
   * After this, we sleep 100ms, until time 400. At this point, job1 has the
   * highest deficit, job2 the second, and job3 the third. The first two tasks
   * should be assigned to job2 and job3 since they are in a pool with an
   * allocation guarantee, but the next two slots should be assigned to job 3
   * because the pool will no longer be needy.
   */
  public void testLargeJobsWithExcessCapacity() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Define the default pool, with minimum of 4 map, 4 reduce
    out.println("<pool name=\"default\">");
    out.println("<minMaps>4</minMaps>");
    out.println("<minReduces>4</minReduces>");
    out.println("</pool>");
    // Give pool A a minimum of 2 maps, 2 reduces
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>2</minMaps>");
    out.println("<minReduces>2</minReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    advanceTime(1000);
    JobInfo info1 = scheduler.getJobInfo(job1);
    
    // Check scheduler variables
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(10,   info1.neededMaps);
    assertEquals(10,   info1.neededReduces);

    assertEquals(4.0,  info1.mapFairShare);
    assertEquals(4.0,  info1.reduceFairShare);
    
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    advanceTime(1000);
    JobInfo info2 = scheduler.getJobInfo(job2);
    
    // Check that minimum and fair shares have been allocated
    assertEquals(4.0,  info1.mapFairShare);
    assertEquals(4.0,  info1.reduceFairShare);
    assertEquals(2.0,  info2.mapFairShare);
    assertEquals(2.0,  info2.reduceFairShare);   

    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    advanceTime(1000);
    JobInfo info3 = scheduler.getJobInfo(job3);
    
    // Check that minimum and fair shares have been allocated
    assertEquals(4.0, info1.mapFairShare);
    assertEquals(4.0, info1.reduceFairShare);
    assertEquals(1.0, info2.mapFairShare);
    assertEquals(1.0, info2.reduceFairShare);
    assertEquals(1.0, info3.mapFairShare);
    assertEquals(1.0, info3.reduceFairShare);
       
    // Assign tasks and check that slots are first given to needy jobs, but
    // that job 1 gets two tasks after due to having a larger deficit.
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");  
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");  
    checkAssignment("tt1", "attempt_test_0003_r_000000_0 on tt1");  
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");  
    checkAssignment("tt2", "attempt_test_0001_r_000000_0 on tt2");  
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");  
    checkAssignment("tt2", "attempt_test_0001_r_000001_0 on tt2");  
  }
  
  /**
   * This test starts by submitting two jobs at time 0:
   * - job1 in the default pool
   * - job2, with 1 map and 1 reduce, in poolA, which has an alloc of 4
   *   maps and 4 reduces
   * 
   * When we assign the slots, job2 should only get 1 of each type of task.
   * 
   * The fair share for job 2 should be 2.0 however, because even though it is
   * running only one task, it accumulates deficit in case it will have failures
   * or need speculative tasks later. (TODO: This may not be a good policy.)
   */
  public void testSmallJobInLargePool() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Define the default pool with minimum of 4 map, 4 reduce
    out.println("<pool name=\"default\">");
    out.println("<minMaps>4</minMaps>");
    out.println("<minReduces>4</minReduces>");
    out.println("</pool>");
    // Give pool A a minimum of 4 maps, 4 reduces
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>4</minMaps>");
    out.println("<minReduces>4</minReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    advanceTime(1000);
    JobInfo info1 = scheduler.getJobInfo(job1);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 1, 1, "poolA");
    advanceTime(1000);
    JobInfo info2 = scheduler.getJobInfo(job2);
    
    // Check scheduler variables
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(10,   info1.neededMaps);
    assertEquals(10,   info1.neededReduces);
    assertEquals(4.0,  info1.mapFairShare);
    assertEquals(4.0,  info1.reduceFairShare);
    assertEquals(0,    info2.runningMaps);
    assertEquals(0,    info2.runningReduces);
    assertEquals(1,    info2.neededMaps);
    assertEquals(1,    info2.neededReduces);
    assertEquals(1.0,  info2.mapFairShare);
    assertEquals(1.0,  info2.reduceFairShare);
    
    // Assign tasks and check that slots are first given to needy jobs
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");  
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");  
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");  
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");  
    checkAssignment("tt2", "attempt_test_0001_r_000001_0 on tt2");  
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2");  
    checkAssignment("tt2", "attempt_test_0001_r_000002_0 on tt2");
  }
  
  /**
   * This test starts by submitting four jobs in the default pool. However, the
   * maxRunningJobs limit for this pool has been set to two. We should see only
   * the first two jobs get scheduled, each with half the total slots.
   */
  public void testPoolMaxJobs() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    out.println("<pool name=\"default\">");
    out.println("<minMaps>4</minMaps>");
    out.println("<minReduces>4</minReduces>");
    out.println("<maxRunningJobs>2</maxRunningJobs>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    advanceTime(1000);
    JobInfo info1 = scheduler.getJobInfo(job1);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    advanceTime(1000);
    JobInfo info2 = scheduler.getJobInfo(job2);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10);
    JobInfo info3 = scheduler.getJobInfo(job3);
    advanceTime(1000);
    JobInProgress job4 = submitJob(JobStatus.RUNNING, 10, 10);
    advanceTime(1000);
    JobInfo info4 = scheduler.getJobInfo(job4);
    
    // Check scheduler variables
    assertEquals(2.0,  info1.mapFairShare);
    assertEquals(2.0,  info1.reduceFairShare);
    assertEquals(2.0,  info2.mapFairShare);
    assertEquals(2.0,  info2.reduceFairShare);
    assertEquals(0.0,  info3.mapFairShare);
    assertEquals(0.0,  info3.reduceFairShare);
    assertEquals(0.0,  info4.mapFairShare);
    assertEquals(0.0,  info4.reduceFairShare);
    
    // Assign tasks and check that slots are first to jobs 1 and 2
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0002_r_000001_0 on tt2");
  }

  /**
   * This test starts by submitting two jobs by user "user1" to the default
   * pool, and two jobs by "user2". We set user1's job limit to 1. We should
   * see one job from user1 and two from user2. 
   */
  public void testUserMaxJobs() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Define the default pool with minimum of 4 map, 4 reduce
    out.println("<pool name=\"default\">");
    out.println("<minMaps>4</minMaps>");
    out.println("<minReduces>4</minReduces>");
    out.println("</pool>");
    out.println("<user name=\"user1\">");
    out.println("<maxRunningJobs>1</maxRunningJobs>");
    out.println("</user>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    // Submit jobs, advancing time in-between to make sure that they are
    // all submitted at distinct times.
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    job1.getJobConf().set("user.name", "user1");
    advanceTime(1000);
    JobInfo info1 = scheduler.getJobInfo(job1);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    advanceTime(1000);
    job2.getJobConf().set("user.name", "user1");
    JobInfo info2 = scheduler.getJobInfo(job2);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10);
    advanceTime(1000);
    job3.getJobConf().set("user.name", "user2");
    JobInfo info3 = scheduler.getJobInfo(job3);
    JobInProgress job4 = submitJob(JobStatus.RUNNING, 10, 10);
    advanceTime(1000);
    job4.getJobConf().set("user.name", "user2");
    JobInfo info4 = scheduler.getJobInfo(job4);
    
    
    // Check scheduler variables
    assertEquals(2.0, info1.mapFairShare);
    assertEquals(2.0, info1.reduceFairShare);
    assertEquals(0.0, info2.mapFairShare);
    assertEquals(0.0, info2.reduceFairShare);
    assertEquals(1.0, info3.mapFairShare);
    assertEquals(1.0, info3.reduceFairShare);
    assertEquals(1.0, info4.mapFairShare);
    assertEquals(1.0, info4.reduceFairShare);

    // Assign tasks and check that slots are first to jobs 1 and 3
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0001_r_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0003_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0003_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0004_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0004_r_000000_0 on tt2");
  }
  
  /**
   * Test a combination of pool job limits and user job limits, the latter
   * specified through both the userMaxJobsDefaults (for some users) and
   * user-specific &lt;user&gt; elements in the allocations file. 
   */
  public void testComplexJobLimits() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Define the default pool with minimum of 4 map, 4 reduce
    out.println("<pool name=\"default\">");
    out.println("<minMaps>4</minMaps>");
    out.println("<minReduces>4</minReduces>");
    out.println("</pool>");
    
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>4</minMaps>");
    out.println("<minReduces>4</minReduces>");
    out.println("<maxRunningJobs>1</maxRunningJobs>");
    out.println("</pool>");
    out.println("<user name=\"user1\">");
    out.println("<maxRunningJobs>1</maxRunningJobs>");
    out.println("</user>");
    out.println("<user name=\"user2\">");
    out.println("<maxRunningJobs>10</maxRunningJobs>");
    out.println("</user>");
    out.println("<userMaxJobsDefault>2</userMaxJobsDefault>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    // Submit jobs, advancing time in-between to make sure that they are
    // all submitted at distinct times.
    
    // Two jobs for user1; only one should get to run
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10);
    job1.getJobConf().set("user.name", "user1");
    advanceTime(1000);
    JobInfo info1 = scheduler.getJobInfo(job1);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10);
    advanceTime(1000);
    job2.getJobConf().set("user.name", "user1");
    JobInfo info2 = scheduler.getJobInfo(job2);
    
    // Three jobs for user2; all should get to run
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10);
    job3.getJobConf().set("user.name", "user2");
    JobInfo info3 = scheduler.getJobInfo(job3);
    advanceTime(1000);
    JobInProgress job4 = submitJob(JobStatus.RUNNING, 10, 10);
    job4.getJobConf().set("user.name", "user2");
    JobInfo info4 = scheduler.getJobInfo(job4);
    advanceTime(1000);
    JobInProgress job5 = submitJob(JobStatus.RUNNING, 10, 10);
    job5.getJobConf().set("user.name", "user2");
    JobInfo info5 = scheduler.getJobInfo(job5);
    advanceTime(1000);
    
    // Three jobs for user3; only two should get to run
    JobInProgress job6 = submitJob(JobStatus.RUNNING, 10, 10);
    job6.getJobConf().set("user.name", "user3");
    JobInfo info6 = scheduler.getJobInfo(job6);
    advanceTime(1000);
    JobInProgress job7 = submitJob(JobStatus.RUNNING, 10, 10);
    job7.getJobConf().set("user.name", "user3");
    JobInfo info7 = scheduler.getJobInfo(job7);
    advanceTime(1000);
    JobInProgress job8 = submitJob(JobStatus.RUNNING, 10, 10);
    job8.getJobConf().set("user.name", "user3");
    JobInfo info8 = scheduler.getJobInfo(job8);
    advanceTime(1000);
    
    // Two jobs for user4, in poolA; only one should get to run
    JobInProgress job9 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    job9.getJobConf().set("user.name", "user4");
    advanceTime(1000);
    JobInfo info9 = scheduler.getJobInfo(job9);
    JobInProgress job10 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    job10.getJobConf().set("user.name", "user4");
    advanceTime(1000);
    JobInfo info10 = scheduler.getJobInfo(job10);
    
    advanceTime(2000);
    // Check scheduler variables
    assertEquals(1.0,  info1.mapFairShare);
    assertEquals(1.0,  info1.reduceFairShare);
    assertEquals(0.0,    info2.mapFairShare);
    assertEquals(0.0,    info2.reduceFairShare);
    assertEquals(1.0,  info3.mapFairShare);
    assertEquals(1.0,  info3.reduceFairShare);
    assertEquals(1.0,  info4.mapFairShare);
    assertEquals(1.0,  info4.reduceFairShare);
    assertEquals(1.0,  info5.mapFairShare);
    assertEquals(1.0,  info5.reduceFairShare);
    assertEquals(0.0,  info6.mapFairShare);
    assertEquals(0.0,  info6.reduceFairShare);
    assertEquals(0.0,  info7.mapFairShare);
    assertEquals(0.0,  info7.reduceFairShare);
    assertEquals(0.0,    info8.mapFairShare);
    assertEquals(0.0,    info8.reduceFairShare);
    assertEquals(4.0,  info9.mapFairShare);
    assertEquals(4.0,  info9.reduceFairShare);
    assertEquals(0.0,    info10.mapFairShare);
    assertEquals(0.0,    info10.reduceFairShare);
  }
  
  public void testSizeBasedWeight() throws Exception {
    scheduler.sizeBasedWeight = true;
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 2, 10);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 50, 1);
    advanceTime(1000);    
    assertTrue(scheduler.getJobInfo(job2).mapFairShare >
               scheduler.getJobInfo(job1).mapFairShare);
    assertTrue(scheduler.getJobInfo(job1).reduceFairShare >
               scheduler.getJobInfo(job2).reduceFairShare);
  }
  
  public void testWaitForMapsBeforeLaunchingReduces() {
    // We have set waitForMapBeforeLaunchingReduces to false by default in
    // this class, so this should return true
    assertTrue(scheduler.enoughMapsFinishedToRunReduces(0, 100));
    
    // However, if we set waitForMapsBeforeLaunchingReduces to true, we should
    // now no longer to be able to assign reduces until 5 have finished
    scheduler.waitForMapsBeforeLaunchingReduces = true;
    assertFalse(scheduler.enoughMapsFinishedToRunReduces(0, 100));
    assertFalse(scheduler.enoughMapsFinishedToRunReduces(1, 100));
    assertFalse(scheduler.enoughMapsFinishedToRunReduces(2, 100));
    assertFalse(scheduler.enoughMapsFinishedToRunReduces(3, 100));
    assertFalse(scheduler.enoughMapsFinishedToRunReduces(4, 100));
    assertFalse(scheduler.enoughMapsFinishedToRunReduces(5, 100));
    assertFalse(scheduler.enoughMapsFinishedToRunReduces(6, 100));
    assertFalse(scheduler.enoughMapsFinishedToRunReduces(7, 100));
    assertFalse(scheduler.enoughMapsFinishedToRunReduces(8, 100));
    assertFalse(scheduler.enoughMapsFinishedToRunReduces(9, 100));
    assertTrue(scheduler.enoughMapsFinishedToRunReduces(10, 100));
    
    // Also test some jobs that have very few maps, in which case we will
    // wait for at least 1 map to finish
    assertFalse(scheduler.enoughMapsFinishedToRunReduces(0, 5));
    assertTrue(scheduler.enoughMapsFinishedToRunReduces(1, 5));
    assertFalse(scheduler.enoughMapsFinishedToRunReduces(0, 1));
    assertTrue(scheduler.enoughMapsFinishedToRunReduces(1, 1));
  }
  
  public void testReduceWatcher() {
    String rawText = "0-1:1;2-3:0.5;default:0.2";
    YTPoolManager.ReduceWatcher watcher = new YTPoolManager.ReduceWatcher(rawText);
    
    // jobLevel is 0
    for (int i = 0; i < 100; i++) {
      assertFalse(watcher.shouldRunReduces(i, 100, 0));
    }
    assertTrue(watcher.shouldRunReduces(100, 100, 0));
    
    // jobLevel is 1
    for (int i = 0; i < 100; i++) {
      assertFalse(watcher.shouldRunReduces(i, 100, 1));
    }
    assertTrue(watcher.shouldRunReduces(100, 100, 1));
    
    // jobLevel = 2
    for (int i = 0; i < 50; i++) {
      assertFalse(watcher.shouldRunReduces(i, 100, 2));
    }
    for (int i = 50; i < 100; i++) {
      assertTrue(watcher.shouldRunReduces(i, 100, 2));
    }
    
    // jobLevel = 3
    for (int i = 0; i < 50; i++) {
      assertFalse(watcher.shouldRunReduces(i, 100, 3));
    }
    for (int i = 50; i < 100; i++) {
      assertTrue(watcher.shouldRunReduces(i, 100, 3));
    }
    
    // jobLevel = 4
    for (int i = 0; i < 20; i++) {
      assertFalse(watcher.shouldRunReduces(i, 100, 4));
    }
    for (int i = 20; i < 100; i++) {
      assertTrue(watcher.shouldRunReduces(i, 100, 4));
    }
  }
  
  private void advanceTime(long time) {
    clock.advance(time);
    scheduler.update();
  }

  protected TaskTrackerStatus tracker(String taskTrackerName) {
    return taskTrackerManager.getTaskTracker(taskTrackerName);
  }
  
  protected void checkAssignment(String taskTrackerName,
      String... expectedTasks) throws IOException {
    List<Task> tasks = scheduler.assignTasks(tracker(taskTrackerName));
    assertNotNull(tasks);
    assertEquals(expectedTasks.length, tasks.size());
    System.out.println("Assigned tasks:");
    for (int i = 0; i < tasks.size(); i++) {
      System.out.println("- " + tasks.get(i));
      assertEquals("assignment " + i, expectedTasks[i], tasks.get(i).toString());
    }
  }

  /**
   * Tests that max-running-tasks per node are set by assigning load
   * equally accross the cluster in CapBasedLoadManager.
   */
  public void testCapBasedLoadManager() {
    YTCapBasedLoadManager loadMgr = new YTCapBasedLoadManager();
    // Arguments to getCap: totalRunnableTasks, nodeCap, totalSlots
    // Desired behavior: return ceil(nodeCap * min(1, runnableTasks/totalSlots))
  }

  /**
   * This test starts by submitting three jobs:
   * - job1 in poolA, with an allocation of 4 maps / 4 reduces
   * - job2 in poolB, with an allocation of 4 maps / 4 reduces
   * - job3 in poolC, with an allocation of 4 maps / 4 reduces
   * 
   * For poolA's weight is 3, poolB's weight is 1 and poolC's weight is 2,
   * there are totally six slots in the scheduling queue of PoolManager.
   * After six heartbeats (one round) poolA should be scheduled three times,
   * poolB one times, poolC two times.
   */
  public void testRoundRobinSchedule() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a minimum of 10 map, 10 reduces and weight 3
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>10</minMaps>");
    out.println("<minReduces>10</minReduces>");
    out.println("<weight>3</weight>");
    out.println("</pool>");
    // Give pool B a minimum of 10 maps, 10 reduce and weight 1
    out.println("<pool name=\"poolB\">");
    out.println("<minMaps>10</minMaps>");
    out.println("<minReduces>10</minReduces>");
    out.println("<weight>1</weight>");
    out.println("</pool>");
    // Give pool C a minimum of 10 maps, 10 reduce and weight 2
    out.println("<pool name=\"poolC\">");
    out.println("<minMaps>10</minMaps>");
    out.println("<minReduces>10</minReduces>");
    out.println("<weight>2</weight>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    // total weights of all pools
    int totalWeights = 6; // 6 = 3 + 1 + 2
    // Map: JobID --> Pool name
    Map<String, String> jobIdPool = new HashMap<String, String>();
    // Map: Pool name --> scheduled task number
    Map<String, Integer> poolTaskNum = new HashMap<String, Integer>();
    
     // submit jobs to poolA, poolB and poolC
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 4, 4, "poolA");
    jobIdPool.put(job1.getJobID().toString(), "poolA");
    poolTaskNum.put("poolA", 0);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 4, 4, "poolB");
    jobIdPool.put(job2.getJobID().toString(), "poolB");
    poolTaskNum.put("poolB", 0);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 4, 4, "poolC");
    jobIdPool.put(job3.getJobID().toString(), "poolC");
    poolTaskNum.put("poolC", 0);
    advanceTime(1000);
    
    // According to round-robin, poolA has 3 times scheduling than poolB,
    // 1.5 times scheduling than poolC
    for (int i = 0; i < totalWeights; i++) {
      List<Task> tasks;
      if (i % 2 == 0) {
        tasks = scheduler.assignTasks(tracker("tt1"));
        assertNotNull("assign tasks on tt1", tasks);
        assertEquals("assign 1 task on tt1", 1, tasks.size());
      } else {
        tasks = scheduler.assignTasks(tracker("tt2"));
        assertNotNull("assign tasks on tt2", tasks);
        assertEquals("assign 1 task on tt2", 1, tasks.size());
      }
      String jobid = tasks.get(0).getJobID().toString();

      String pool = jobIdPool.get(jobid);
      
      poolTaskNum.put(pool, poolTaskNum.get(pool) + 1);

      if (i % 2 == 0) {
        taskTrackerManager.finishTask("tt1", tasks.get(0).getTaskID().toString());
      } else {
        taskTrackerManager.finishTask("tt2", tasks.get(0).getTaskID().toString());
      }
    }
 
    assertEquals(3, poolTaskNum.get("poolA").intValue());
    assertEquals(1, poolTaskNum.get("poolB").intValue());
    assertEquals(2, poolTaskNum.get("poolC").intValue());
  }
  
  /**
   * This test starts by submitting three jobs:
   * - job1 in poolA, with an allocation of 4 maps / 4 reduces
   * - job2 in poolB, with an allocation of 4 maps / 4 reduces
   * - job3 in poolC, with an allocation of 4 maps / 4 reduces
   * 
   * For poolA's weight is 3, poolB's weight is 1 and poolC's weight is 2,
   * there are totally six slots in the scheduling queue of PoolManager.
   * After six heartbeats (one round) poolA should be scheduled three times,
   * poolB one times, poolC two times.
   */
  public void testRoundRobinScheduleWithAssignMultiple() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a minimum of 10 map, 10 reduces and weight 3
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>10</minMaps>");
    out.println("<minReduces>10</minReduces>");
    out.println("<weight>3</weight>");
    out.println("</pool>");
    // Give pool B a minimum of 10 maps, 10 reduce and weight 1
    out.println("<pool name=\"poolB\">");
    out.println("<minMaps>10</minMaps>");
    out.println("<minReduces>10</minReduces>");
    out.println("<weight>1</weight>");
    out.println("</pool>");
    // Give pool C a minimum of 10 maps, 10 reduce and weight 2
    out.println("<pool name=\"poolC\">");
    out.println("<minMaps>10</minMaps>");
    out.println("<minReduces>10</minReduces>");
    out.println("<weight>2</weight>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    setUpCluster(1, 2, true);
    
    // total weights of all pools
    int totalWeights = 3; // 3 = (3 + 1 + 2)/2
    // Map: JobID --> Pool name
    Map<String, String> jobIdPool = new HashMap<String, String>();
    // Map: Pool name --> scheduled task number
    Map<String, Integer> poolTaskNum = new HashMap<String, Integer>();
    
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 10, "poolA");
    jobIdPool.put(job1.getJobID().toString(), "poolA");
    poolTaskNum.put("poolA", 0);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 10, "poolB");
    jobIdPool.put(job2.getJobID().toString(), "poolB");
    poolTaskNum.put("poolB", 0);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 10, "poolC");
    jobIdPool.put(job3.getJobID().toString(), "poolC");
    poolTaskNum.put("poolC", 0);
    advanceTime(1000);
    
    // According to round-robin, poolA has 3 times scheduling than poolB,
    // 1.5 times scheduling than poolC
    for (int i = 0; i < totalWeights; i++) {
      List<Task> tasks;
      String tt = "tt" + String.valueOf(i%2+1);
      tasks = scheduler.assignTasks(tracker(tt));
      assertNotNull("assign tasks on tt1", tasks);
      System.out.println("To " + tt + " assigned tasks:");
      for (Task t : tasks) {
        String jobid = t.getJobID().toString();
        String pool = jobIdPool.get(jobid);
        poolTaskNum.put(pool, poolTaskNum.get(pool) + 1);
        System.out.println("- " + t);
        taskTrackerManager.finishTask(tt, t.getTaskID().toString());
      }
    }
 
    assertEquals(6, poolTaskNum.get("poolA").intValue());
    assertEquals(2, poolTaskNum.get("poolB").intValue());
    assertEquals(4, poolTaskNum.get("poolC").intValue());
  }
  
  /**
   * This test starts by submitting three jobs:
   * - job1 in poolA, with an allocation of 2 maps / 2 reduces
   * - job2 in poolB, with an allocation of 2 maps / 2 reduces
   * - job3 in poolC, with an allocation of 2 maps / 2 reduces
   * 
   * Although poolA's weight is 3, poolB's weight is 1 and poolC's weight is 2,
   * poolD's weight is 4, we can schedule all the tasks in six heartbeats.
   */
  public void testSkipEmptyPoolSchedule() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Give pool A a minimum of 10 map, 10 reduces and weight 3
    out.println("<pool name=\"poolA\">");
    out.println("<minMaps>10</minMaps>");
    out.println("<minReduces>10</minReduces>");
    out.println("<weight>3</weight>");
    out.println("</pool>");
    // Give pool B a minimum of 10 maps, 10 reduce and weight 1
    out.println("<pool name=\"poolB\">");
    out.println("<minMaps>10</minMaps>");
    out.println("<minReduces>10</minReduces>");
    out.println("<weight>1</weight>");
    out.println("</pool>");
    // Give pool C a minimum of 10 maps, 10 reduce and weight 2
    out.println("<pool name=\"poolC\">");
    out.println("<minMaps>10</minMaps>");
    out.println("<minReduces>10</minReduces>");
    out.println("<weight>2</weight>");
    out.println("</pool>");
    // Give pool D a minimum of 10 maps, 10 reduce and weight 4
    out.println("<pool name=\"poolD\">");
    out.println("<minMaps>10</minMaps>");
    out.println("<minReduces>10</minReduces>");
    out.println("<weight>4</weight>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    
    // Map: JobID --> Pool name
    Map<String, String> jobIdPool = new HashMap<String, String>();
    // Map: Pool name --> scheduled task number
    Map<String, Integer> poolTaskNum = new HashMap<String, Integer>();
    
     // submit jobs to poolA, poolB and poolC
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 10, 0, "poolA");
    jobIdPool.put(job1.getJobID().toString(), "poolA");
    poolTaskNum.put("poolA", 0);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 0, "poolB");
    jobIdPool.put(job2.getJobID().toString(), "poolB");
    poolTaskNum.put("poolB", 0);
    JobInProgress job3 = submitJob(JobStatus.RUNNING, 10, 0, "poolC");
    jobIdPool.put(job3.getJobID().toString(), "poolC");
    poolTaskNum.put("poolC", 0);
    poolTaskNum.put("poolD", 0);
    advanceTime(1000);
    
    // We can schedule all the tasks in six heartbeats.
    for (int i = 0; i < 6; i++) {
      List<Task> tasks;
      if (i % 2 == 0) {
        tasks = scheduler.assignTasks(tracker("tt1"));
        assertNotNull("assign tasks on tt1", tasks);
        assertEquals("assign 1 task on tt1", 1, tasks.size());
      } else {
        tasks = scheduler.assignTasks(tracker("tt2"));
        assertNotNull("assign tasks on tt2", tasks);
        assertEquals("assign 1 task on tt2", 1, tasks.size());
      }
      String jobid = tasks.get(0).getJobID().toString();

      String pool = jobIdPool.get(jobid);
      
      poolTaskNum.put(pool, poolTaskNum.get(pool) + 1);

      if (i % 2 == 0) {
        taskTrackerManager.finishTask("tt1", tasks.get(0).getTaskID().toString());
      } else {
        taskTrackerManager.finishTask("tt2", tasks.get(0).getTaskID().toString());
      }
    }
 
    assertEquals(3, poolTaskNum.get("poolA").intValue());
    assertEquals(1, poolTaskNum.get("poolB").intValue());
    assertEquals(2, poolTaskNum.get("poolC").intValue());
    assertEquals(0, poolTaskNum.get("poolD").intValue());
  }
  
  /**
   * We submit two jobs such that one has 2x the level of the other
   * and check that the levels are okay. The higher level job will be 
   * scheduled first.
   */
  public void testJobsWithLevels() throws IOException {
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 2, 2, null, 0);
    JobInfo info1 = scheduler.getJobInfo(job1);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 2, 2, null, 1);
    JobInfo info2 = scheduler.getJobInfo(job2);
    advanceTime(1000);
    scheduler.update();
    
    // Check scheduler variables
    assertEquals(0,    info1.runningMaps);
    assertEquals(0,    info1.runningReduces);
    assertEquals(2,   info1.neededMaps);
    assertEquals(2,   info1.neededReduces);
    assertEquals(0,    info1.jobLevel);
    assertEquals(0,    info2.runningMaps);
    assertEquals(0,    info2.runningReduces);
    assertEquals(2,   info2.neededMaps);
    assertEquals(2,   info2.neededReduces);
    assertEquals(1,    info2.jobLevel);
 
    // Assign tasks and check that all slots are filled with j1, then j2
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000000_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_m_000001_0 on tt1");
    checkAssignment("tt1", "attempt_test_0002_r_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000000_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2");
    checkAssignment("tt2", "attempt_test_0001_r_000001_0 on tt2");
  }
  
  /**
   * This test exercises delay scheduling at the node level. We submit a job
   * with data on rack1.node2 and check that it doesn't get assigned on earlier
   * nodes. A second job with no locality info should get assigned instead.
   * 
   * TaskTracker names in this test map to nodes as follows:
   * - tt1 = rack1.node1
   * - tt2 = rack1.node2
   * - tt3 = rack2.node1
   * - tt4 = rack2.node2
   */
  public void testDelaySchedulingAtNodeLevel() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Define the default pool with minimum of 4 map, 4 reduces
    out.println("<pool name=\"default\">");
    out.println("<minMaps>10</minMaps>");
    out.println("<minReduces>10</minReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    setUpCluster(2, 2, true);
    
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 1, 0, null,
        new String[][] {
          {"rack2.node2"}
        });
    JobInfo info1 = scheduler.getJobInfo(job1);
    
    // Advance time before submitting another job j2, to make j1 be ahead
    // of j2 in the queue deterministically.
    advanceTime(1000);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 10, 0);
    advanceTime(1000);
    
    // Assign tasks on nodes 1-3 and check that j2 gets them
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1", 
                           "attempt_test_0002_m_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000002_0 on tt2",
                           "attempt_test_0002_m_000003_0 on tt2");
    checkAssignment("tt3", "attempt_test_0002_m_000004_0 on tt3",
                           "attempt_test_0002_m_000005_0 on tt3");
    
    // Assign a task on node 4 now and check that j1 gets it. The other slot
    // on the node should be given to j2 because j1 will be out of tasks.
    checkAssignment("tt4", "attempt_test_0001_m_000000_0 on tt4",
                           "attempt_test_0002_m_000006_0 on tt4");
    
    // Check that delay scheduling info is properly set
    assertEquals(info1.lastMapLocalityLevel, LocalityLevel.NODE);
    assertEquals(info1.timeWaitedForLocalMap, 0);
    assertEquals(info1.skippedAtLastHeartbeat > 0
        && info1.assignedAtLastHeartbeat == 0, false);
  }
  
  /**
   * This test submits a job and causes it to exceed its node-level delay,
   * and thus to go on to launch a rack-local task. We submit one job with data
   * on rack2.node4 and check that it does not get assigned on any of the other
   * nodes until 10 seconds (the delay configured in setUpCluster) pass.
   * Finally, after some delay, we let the job assign local tasks and check
   * that it has returned to waiting for node locality.
   * 
   * TaskTracker names in this test map to nodes as follows:
   * - tt1 = rack1.node1
   * - tt2 = rack1.node2
   * - tt3 = rack2.node1
   * - tt4 = rack2.node2
   */
  public void testDelaySchedulingAtRackLevel() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Define the default pool with minimum of 4 map, 4 reduces
    out.println("<pool name=\"default\">");
    out.println("<minMaps>10</minMaps>");
    out.println("<minReduces>10</minReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    setUpCluster(2, 2, true);
    
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 4, 0, null,
        new String[][] {
          {"rack2.node2"}, {"rack2.node2"}, {"rack2.node2"}, {"rack2.node2"}
        });
    JobInfo info1 = scheduler.getJobInfo(job1);
    
    // Advance time before submitting another job j2, to make j1 be ahead
    // of j2 in the queue deterministically.
    advanceTime(100);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 20, 0);
    advanceTime(100);
    
    // Assign tasks on nodes 1-3 and check that j2 gets them
    checkAssignment("tt1", "attempt_test_0002_m_000000_0 on tt1", 
                           "attempt_test_0002_m_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000002_0 on tt2",
                           "attempt_test_0002_m_000003_0 on tt2");
    checkAssignment("tt3", "attempt_test_0002_m_000004_0 on tt3",
                           "attempt_test_0002_m_000005_0 on tt3");
    
    // Advance time by 11 seconds to put us past the 10-second locality delay
    advanceTime(11000);
    
    // Finish some tasks on each node
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000000_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_m_000002_0");
    taskTrackerManager.finishTask("tt3", "attempt_test_0002_m_000004_0");
    advanceTime(100);
    
    // Check that job 1 is only assigned on node 3 (which is rack-local)
    checkAssignment("tt1", "attempt_test_0002_m_000006_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000007_0 on tt2");
    checkAssignment("tt3", "attempt_test_0001_m_000000_0 on tt3");
    
    // Check that delay scheduling info is properly set
    assertEquals(info1.lastMapLocalityLevel, LocalityLevel.RACK);
    assertEquals(info1.timeWaitedForLocalMap, 0);
    assertEquals(info1.skippedAtLastHeartbeat > 0
        && info1.assignedAtLastHeartbeat == 0, false);
    
    // Also give job 1 some tasks on node 4. Its lastMapLocalityLevel
    // should go back to 0 after it gets assigned these.
    checkAssignment("tt4", "attempt_test_0001_m_000001_0 on tt4",
                           "attempt_test_0001_m_000002_0 on tt4");
    
    // Check that delay scheduling info is properly set
    assertEquals(info1.lastMapLocalityLevel, LocalityLevel.NODE);
    assertEquals(info1.timeWaitedForLocalMap, 0);
    assertEquals(info1.skippedAtLastHeartbeat > 0
        && info1.assignedAtLastHeartbeat == 0, false);
    
    // Check that job 1 no longer assigns tasks in the same rack now
    // that it has obtained a node-local task
    taskTrackerManager.finishTask("tt1", "attempt_test_0002_m_000001_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0002_m_000003_0");
    taskTrackerManager.finishTask("tt3", "attempt_test_0002_m_000005_0");
    advanceTime(100);
    checkAssignment("tt1", "attempt_test_0002_m_000008_0 on tt1");
    checkAssignment("tt2", "attempt_test_0002_m_000009_0 on tt2");
    checkAssignment("tt3", "attempt_test_0002_m_000010_0 on tt3");
    advanceTime(100);
    
    // However, job 1 should still be able to launch tasks on node 4
    taskTrackerManager.finishTask("tt4", "attempt_test_0001_m_000001_0");
    advanceTime(100);
    checkAssignment("tt4", "attempt_test_0001_m_000003_0 on tt4");
  }
  
  /**
   * This test submits a job and causes it to exceed its node-level delay,
   * then its rack-level delay. It should then launch tasks off-rack.
   * However, once the job gets a rack-local slot it should stay in-rack,
   * and once it gets a node-local slot it should stay in-node.
   * For simplicity, we don't submit a second job in this test.
   * 
   * TaskTracker names in this test map to nodes as follows:
   * - tt1 = rack1.node1
   * - tt2 = rack1.node2
   * - tt3 = rack2.node1
   * - tt4 = rack2.node2
   */
  public void testDelaySchedulingOffRack() throws Exception {
    // Set up pools file
    PrintWriter out = new PrintWriter(new FileWriter(ALLOC_FILE));
    out.println("<?xml version=\"1.0\"?>");
    out.println("<allocations>");
    // Define the default pool with minimum of 4 map, 4 reduces
    out.println("<pool name=\"default\">");
    out.println("<minMaps>10</minMaps>");
    out.println("<minReduces>10</minReduces>");
    out.println("</pool>");
    out.println("</allocations>");
    out.close();
    scheduler.getPoolManager().reloadAllocs();
    setUpCluster(2, 2, true);
    
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 8, 0, null,
        new String[][] {
          {"rack2.node2"}, {"rack2.node2"}, {"rack2.node2"}, {"rack2.node2"},
          {"rack2.node2"}, {"rack2.node2"}, {"rack2.node2"}, {"rack2.node2"},
        });
    JobInfo info1 = scheduler.getJobInfo(job1);
    advanceTime(100);
    
    // Check that nothing is assigned on trackers 1-3
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
    assertNull(scheduler.assignTasks(tracker("tt3")));
    
    // Advance time by 11 seconds to put us past the 10-sec node locality delay
    advanceTime(11000);

    // Check that nothing is assigned on trackers 1-2; the job would assign
    // a task on tracker 3 (rack1.node2) so we skip that one 
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
    
    // Repeat to see that receiving multiple heartbeats works
    advanceTime(100);
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
    advanceTime(100);
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));

    // Check that delay scheduling info is properly set
    assertEquals(info1.lastMapLocalityLevel, LocalityLevel.NODE);
    assertEquals(info1.timeWaitedForLocalMap, 11200);
    assertEquals(info1.skippedAtLastHeartbeat > 0
        && info1.assignedAtLastHeartbeat == 0, true);
    
    // Advance time by 11 seconds to put us past the 10-sec rack locality delay
    advanceTime(11000);
    
    // Now the job should be able to assign tasks on tt1 and tt2
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1",
                           "attempt_test_0001_m_000001_0 on tt1");
    checkAssignment("tt2", "attempt_test_0001_m_000002_0 on tt2",
                           "attempt_test_0001_m_000003_0 on tt2");

    // Check that delay scheduling info is properly set
    assertEquals(info1.lastMapLocalityLevel, LocalityLevel.ANY);
    assertEquals(info1.timeWaitedForLocalMap, 0);
    assertEquals(info1.skippedAtLastHeartbeat > 0
        && info1.assignedAtLastHeartbeat == 0, false);
    
    // Now assign a task on tt3. This should make the job stop assigning
    // on tt1 and tt2 (checked after we finish some tasks there)
    checkAssignment("tt3", "attempt_test_0001_m_000004_0 on tt3",
                           "attempt_test_0001_m_000005_0 on tt3");

    // Check that delay scheduling info is properly set
    assertEquals(info1.lastMapLocalityLevel, LocalityLevel.RACK);
    assertEquals(info1.timeWaitedForLocalMap, 0);
    assertEquals(info1.skippedAtLastHeartbeat > 0
        && info1.assignedAtLastHeartbeat == 0, false);
    
    // Check that j1 no longer assigns tasks on rack 1 now
    taskTrackerManager.finishTask("tt1", "attempt_test_0001_m_000001_0");
    taskTrackerManager.finishTask("tt2", "attempt_test_0001_m_000003_0");
    advanceTime(100);
    assertNull(scheduler.assignTasks(tracker("tt1")));
    assertNull(scheduler.assignTasks(tracker("tt2")));
    
    // However, tasks on rack 2 should still be assigned
    taskTrackerManager.finishTask("tt3", "attempt_test_0001_m_000004_0");
    advanceTime(100);
    checkAssignment("tt3", "attempt_test_0001_m_000006_0 on tt3");
    
    // Now assign a task on node 4
    checkAssignment("tt4", "attempt_test_0001_m_000007_0 on tt4");

    // Check that delay scheduling info is set so we are looking for node-local
    // tasks at this point
    assertEquals(info1.lastMapLocalityLevel, LocalityLevel.NODE);
    assertEquals(info1.timeWaitedForLocalMap, 0);
    assertEquals(info1.skippedAtLastHeartbeat > 0
        && info1.assignedAtLastHeartbeat == 0, false);
  }
  
  /**
   * This test submits two jobs with 4 maps and 3 reduces in total to a
   * 4-node cluster. Although the cluster has 2 map slots and 2 reduce
   * slots per node, it should only launch one map and one reduce on each
   * node to balance the load. We check that this happens even if
   * assignMultiple is set to true so the scheduler has the opportunity
   * to launch multiple tasks per heartbeat.
   */
  public void testAssignMultipleWithUnderloadedCluster() throws IOException {
    setUpCluster(1, 4, true);
    JobInProgress job1 = submitJob(JobStatus.RUNNING, 2, 2);
    
    // Advance to make j1 be scheduled before j2 deterministically.
    advanceTime(100);
    JobInProgress job2 = submitJob(JobStatus.RUNNING, 2, 1);
    advanceTime(100);
    
    // Assign tasks and check that at most one map and one reduce slot is used
    // on each node, and that no tasks are assigned on subsequent heartbeats
    checkAssignment("tt1", "attempt_test_0001_m_000000_0 on tt1",
                           "attempt_test_0001_r_000000_0 on tt1");
    assertNull(scheduler.assignTasks(tracker("tt1")));
    checkAssignment("tt2", "attempt_test_0001_m_000001_0 on tt2",
                           "attempt_test_0001_r_000001_0 on tt2");
    assertNull(scheduler.assignTasks(tracker("tt2")));
    checkAssignment("tt3", "attempt_test_0002_m_000000_0 on tt3",
                           "attempt_test_0002_r_000000_0 on tt3");
    assertNull(scheduler.assignTasks(tracker("tt3")));
    checkAssignment("tt4", "attempt_test_0002_m_000001_0 on tt4");
    assertNull(scheduler.assignTasks(tracker("tt4")));
  }
  
  /**
   * When add a job to a pool, we enqueue the job according its level,
   * start time and job id respectively.
   */
  public void testLevelBasedFIFOJobsInPool() throws IOException{
    YTPool pool = new YTPool("test");
    JobConf jobConf = new JobConf();
    
    // Insert three jobs with level five
    jobConf.setInt(JOB_LEVEL, 5);
    FakeJobInProgress job0 = new FakeJobInProgress(jobConf, null, null, null);
    pool.addJob(job0);
    FakeJobInProgress job1 = new FakeJobInProgress(jobConf, null, null, null);
    pool.addJob(job1);
    FakeJobInProgress job2 = new FakeJobInProgress(jobConf, null, null, null);
    pool.addJob(job2);
    
    // Test fifo with same level
    Set<JobInProgress> jobs = pool.getOrderedJobs();
    Iterator<JobInProgress> it = jobs.iterator();
    assertEquals(job0, it.next());
    assertEquals(job1, it.next());
    assertEquals(job2, it.next());
    
    // Insert two jobs with level four
    jobConf.setInt(JOB_LEVEL, 4);
    FakeJobInProgress job3 = new FakeJobInProgress(jobConf, null, null, null);
    pool.addJob(job3);
    FakeJobInProgress job4 = new FakeJobInProgress(jobConf, null, null, null);
    pool.addJob(job4);
    
    // Test level 5 job is above jobs with level 4
    jobs = pool.getOrderedJobs();
    it = jobs.iterator();
    assertEquals(job0, it.next());
    assertEquals(job1, it.next());
    assertEquals(job2, it.next());
    assertEquals(job3, it.next());
    assertEquals(job4, it.next());
    
    // Insert two jobs with level six
    jobConf.setInt(JOB_LEVEL, 6);
    FakeJobInProgress job5 = new FakeJobInProgress(jobConf, null, null, null);
    pool.addJob(job5);
    FakeJobInProgress job6 = new FakeJobInProgress(jobConf, null, null, null);
    pool.addJob(job6);
    
    // Test jobs order 6 6 > 5 5 5 > 4 4
    jobs = pool.getOrderedJobs();
    it = jobs.iterator();
    assertEquals(job5, it.next());
    assertEquals(job6, it.next());
    assertEquals(job0, it.next());
    assertEquals(job1, it.next());
    assertEquals(job2, it.next());
    assertEquals(job3, it.next());
    assertEquals(job4, it.next());
  }
}
