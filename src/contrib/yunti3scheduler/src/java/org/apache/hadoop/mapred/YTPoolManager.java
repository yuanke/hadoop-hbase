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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Yunti3Scheduler.JobInfo;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

/**
 * Maintains a hierarchy of pools.
 */
public class YTPoolManager {
  public static final Log LOG = LogFactory.getLog(
    "org.apache.hadoop.mapred.PoolManager");

  /** Time to wait between checks of the allocation file */
  public static final long ALLOC_RELOAD_INTERVAL = 10 * 1000;
  
  /**
   * Time to wait after the allocation has been modified before reloading it
   * (this is done to prevent loading a file that hasn't been fully written).
   */
  public static final long ALLOC_RELOAD_WAIT = 5 * 1000; 
  
  // Map and reduce minimum allocations for each pool
  private Map<String, Integer> mapAllocs = new HashMap<String, Integer>();
  private Map<String, Integer> reduceAllocs = new HashMap<String, Integer>();
  
  // Max concurrent running jobs for each pool and for each user; in addition,
  // for users that have no max specified, we use the userMaxJobsDefault.
  private Map<String, Integer> poolMaxJobs = new HashMap<String, Integer>();
  private Map<String, Integer> userMaxJobs = new HashMap<String, Integer>();
  
  // Sharing weights for each pool 
  private Map<String, Integer> poolWeights = new HashMap<String, Integer>();
  
  // whether use FIFO
  private Map<String, Boolean> poolUseFIFO = new HashMap<String, Boolean>();
  
  // Map: pool name -> ReduceWatcher object.
  private Map<String, ReduceWatcher> poolReduceWatcher = new HashMap<String, ReduceWatcher>();
  
  private int poolWeightDefault = 1;
  
   // A list of ordered pools to be scheduled. A pool with 2x weight has two slots. 
  private List<YTPool> orderedPools = new ArrayList<YTPool>();
  
  // current scheduled pool's index in orderedPools
  private int poolIndexLastAccessed;
  
  //too big will cause out of memory when doing: List<JobInProgress> nextRunnableJobs = new ArrayList<JobInProgress>(cntPoolMaxJobs);
  private final int POOL_MAX_JOBS_DEFAULT = 32;
  private int userMaxJobsDefault = 32;

  private String allocFile; // Path to XML file containing allocations
  private String poolNameProperty; // Jobconf property to use for determining a
                                   // job's pool name (default: mapred.job.queue.name)
  
  private Map<String, YTPool> pools = new HashMap<String, YTPool>();
  
  private long lastReloadAttempt; // Last time we tried to reload the pools file
  private long lastSuccessfulReload; // Last time we successfully reloaded pools
  private boolean lastReloadAttemptFailed = false;
  
  private URI siteConf;
  private long lastSuccessfulReloadSiteConf;

  static class ReduceWatcher {
    private Map<Integer, Double> levelPercent = new HashMap<Integer, Double>();
    private double defaultPercent = 0.1;
    
    public ReduceWatcher() {
      
    }
    // Example: 0-1:1;2-3:0.5;default:0.1
    public ReduceWatcher(String rawText) {
      try {
        String[] items = rawText.split(";");
        for (String item : items) {
          String[] pair = item.split(":", 2);
          double percent = Double.parseDouble(pair[1]);
          if ("default".equals(pair[0])) {
            defaultPercent = percent;
          } else {
            String[] range = pair[0].split("-", 2);
            int start = Integer.parseInt(range[0]);
            int end = Integer.parseInt(range[1]);
            for (int i = start; i <= end; i++) {
              levelPercent.put(i, percent);
            }
          }
        }
      } catch (Exception e) {
        levelPercent.clear();
        LOG.warn("Catch Exception when parse '" + rawText + "', use default instead. " + e.getMessage());
      }
    }
    
    public boolean shouldRunReduces(int finishedMaps, int totalMaps, int jobLevel) {
      Double percent = levelPercent.get(jobLevel);
      if (percent == null) {
        percent = defaultPercent;
      }
      return finishedMaps >= Math.max(1, totalMaps * percent);
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("default: " + defaultPercent);
      for (Map.Entry<Integer, Double> entry : levelPercent.entrySet()) {
        sb.append(" Level: " + entry.getKey() + "->" + entry.getValue());
      }
      return sb.toString();
    }
  }
  
  public YTPoolManager(Configuration conf) throws IOException, SAXException,
      YTAllocationConfigurationException, ParserConfigurationException {
    this.poolNameProperty = conf.get(
        "mapred.yunti3scheduler.poolnameproperty", "mapred.job.queue.name");
    this.allocFile = conf.get("mapred.yunti3scheduler.allocation.file");
    if (allocFile == null) {
      LOG.warn("No mapred.yunti3scheduler.allocation.file given in jobconf - " +
          "the fair scheduler will not use any queues.");
    }
    reloadAllocs();
    lastSuccessfulReload = System.currentTimeMillis();
    lastReloadAttempt = System.currentTimeMillis();
    lastSuccessfulReloadSiteConf = System.currentTimeMillis();
    
    try {
      siteConf = conf.getResource("hadoop-site.xml").toURI();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * Get a pool by name, creating it if necessary
   */
  public synchronized YTPool getPool(String name) {
    YTPool pool = pools.get(name);
    if (pool == null) {
      pool = new YTPool(name);
      pools.put(name, pool);
    }
    return pool;
  }

  /**
   * Remove a pool by name
   */
  public synchronized void removePool(String name) {
    pools.remove(name);
  }
  
  /**
   * Reload allocations file if it hasn't been loaded in a while
   */
  public void reloadAllocsIfNecessary(TaskTrackerManager taskTrackerManager) {
    long time = System.currentTimeMillis();
    if (time > lastReloadAttempt + ALLOC_RELOAD_INTERVAL) {
      lastReloadAttempt = time;
      try {
        File file = new File(allocFile);
        long lastModified = file.lastModified();
        if (lastModified > lastSuccessfulReload &&
            time > lastModified + ALLOC_RELOAD_WAIT) {
          reloadAllocs();
          
          lastSuccessfulReload = time;
          lastReloadAttemptFailed = false;
        }
      } catch (Exception e) {
        // Throwing the error further out here won't help - the RPC thread
        // will catch it and report it in a loop. Instead, just log it and
        // hope somebody will notice from the log.
        // We log the error only on the first failure so we don't fill up the
        // JobTracker's log with these messages.
        if (!lastReloadAttemptFailed) {
          LOG.error("Failed to reload allocations file - " +
              "will use existing allocations.", e);
        }
        lastReloadAttemptFailed = true;
      }
      
      // reload queue info from hadoop-site.xml
      // added by megatron
      try {
    	  File file = new File(siteConf);
    	  long lastModified = file.lastModified();
	      if(lastModified > lastSuccessfulReloadSiteConf &&
	              time > lastModified + ALLOC_RELOAD_WAIT) {
	        QueueManager queueManager = taskTrackerManager.getQueueManager();
	        queueManager.refresh(new Configuration());

	        // reset schedulerInfo of queueManager added by liangly
	        if (taskTrackerManager instanceof JobTracker) {
	          TaskScheduler scheduler = ((JobTracker) taskTrackerManager)
	          .getTaskScheduler();
	          if (scheduler instanceof Yunti3Scheduler) {
	            ((Yunti3Scheduler) scheduler).setSchedulerInfo();
	          }
	        }
	        
	        lastSuccessfulReloadSiteConf = time;
	      }
      } catch (Exception e) {
    	  LOG.error("queueManager.refresh error", e);
      }
    }
  }
  
  /**
   * Updates the allocation list from the allocation config file. This file is
   * expected to be in the following whitespace-separated format:
   * 
   * <code>
   * poolName1 mapAlloc reduceAlloc
   * poolName2 mapAlloc reduceAlloc
   * ...
   * </code>
   * 
   * Blank lines and lines starting with # are ignored.
   *  
   * @throws IOException if the config file cannot be read.
   * @throws YTAllocationConfigurationException if allocations are invalid.
   * @throws ParserConfigurationException if XML parser is misconfigured.
   * @throws SAXException if config file is malformed.
   */
  public void reloadAllocs() throws IOException, ParserConfigurationException, 
      SAXException, YTAllocationConfigurationException {
    if (allocFile == null) return;
    // Create some temporary hashmaps to hold the new allocs, and we only save
    // them in our fields if we have parsed the entire allocs file successfully.
    Map<String, Integer> mapAllocs = new HashMap<String, Integer>();
    Map<String, Integer> reduceAllocs = new HashMap<String, Integer>();
    Map<String, Integer> poolMaxJobs = new HashMap<String, Integer>();
    Map<String, Integer> userMaxJobs = new HashMap<String, Integer>();
    Map<String, Integer> poolWeights = new HashMap<String, Integer>();
    Map<String, Boolean> poolUseFIFO = new HashMap<String, Boolean>();
    Map<String, ReduceWatcher> poolReduceWatcher = new HashMap<String, ReduceWatcher>();
    int userMaxJobsDefault = POOL_MAX_JOBS_DEFAULT;
    
    // Remember all pool names so we can display them on web UI, etc.
    List<String> poolNamesInAllocFile = new ArrayList<String>();
    
    // Read and parse the allocations file.
    DocumentBuilderFactory docBuilderFactory =
      DocumentBuilderFactory.newInstance();
    docBuilderFactory.setIgnoringComments(true);
    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
    Document doc = builder.parse(new File(allocFile));
    Element root = doc.getDocumentElement();
    if (!"allocations".equals(root.getTagName()))
      throw new YTAllocationConfigurationException("Bad allocations file: " + 
          "top-level element not <allocations>");
    NodeList elements = root.getChildNodes();
    for (int i = 0; i < elements.getLength(); i++) {
      Node node = elements.item(i);
      if (!(node instanceof Element))
        continue;
      Element element = (Element)node;
      if ("pool".equals(element.getTagName())) {
        String poolName = element.getAttribute("name");
        poolNamesInAllocFile.add(poolName);
        NodeList fields = element.getChildNodes();
        for (int j = 0; j < fields.getLength(); j++) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element))
            continue;
          Element field = (Element) fieldNode;
          if ("minMaps".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            int val = Integer.parseInt(text);
            mapAllocs.put(poolName, val);
          } else if ("minReduces".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            int val = Integer.parseInt(text);
            reduceAllocs.put(poolName, val);
          } else if ("maxRunningJobs".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            int val = Integer.parseInt(text);
            poolMaxJobs.put(poolName, val);
          } else if ("weight".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            int val = Integer.parseInt(text);
            poolWeights.put(poolName, val);
          } else if ("useFIFO".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            boolean val = Boolean.parseBoolean(text);
            poolUseFIFO.put(poolName, val);
          } else if ("lazyReduce".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            ReduceWatcher watcher = new ReduceWatcher(text);
            poolReduceWatcher.put(poolName, watcher);
          }
        }
      } else if ("user".equals(element.getTagName())) {
        String userName = element.getAttribute("name");
        NodeList fields = element.getChildNodes();
        for (int j = 0; j < fields.getLength(); j++) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element))
            continue;
          Element field = (Element) fieldNode;
          if ("maxRunningJobs".equals(field.getTagName())) {
            String text = ((Text)field.getFirstChild()).getData().trim();
            int val = Integer.parseInt(text);
            userMaxJobs.put(userName, val);
          }
        }
      } else if ("userMaxJobsDefault".equals(element.getTagName())) {
        String text = ((Text)element.getFirstChild()).getData().trim();
        int val = Integer.parseInt(text);
        userMaxJobsDefault = val;
      } else {
        LOG.warn("Bad element in allocations file: " + element.getTagName());
      }
    }
    
    // Commit the reload; also create any pool defined in the alloc file
    // if it does not already exist, so it can be displayed on the web UI.
    synchronized(this) {
      this.mapAllocs = mapAllocs;
      this.reduceAllocs = reduceAllocs;
      this.poolMaxJobs = poolMaxJobs;
      this.userMaxJobs = userMaxJobs;
      this.userMaxJobsDefault = userMaxJobsDefault;
      this.poolWeights = poolWeights;
      this.poolUseFIFO = poolUseFIFO;
      this.poolReduceWatcher = poolReduceWatcher;

      // remove deleted queue
      Set<String> uncontainedPoolNameSet = new HashSet<String>();
      for (String key: pools.keySet())
        uncontainedPoolNameSet.add(key);
      
      for(String name : poolNamesInAllocFile.toArray(new String[0])){
        if(uncontainedPoolNameSet.contains(name)){
          uncontainedPoolNameSet.remove(name);
        }
      }
      for(String name : uncontainedPoolNameSet){
        if(getPool(name).getJobs().size() == 0)
          pools.remove(name);
      }
      uncontainedPoolNameSet = null;

      for (String name: poolNamesInAllocFile) {
        getPool(name);
      }
            
      // create a list of ordered pools to be scheduled. 
      this.orderedPools = orderPools();      
      this.poolIndexLastAccessed = 0;      
    }
  }

  /**
   * Get the allocation for a particular pool
   */
  public int getAllocation(String pool, YTTaskType taskType) {
    Map<String, Integer> allocationMap = (taskType == YTTaskType.MAP ?
        mapAllocs : reduceAllocs);
    Integer alloc = allocationMap.get(pool);
    return (alloc == null ? 0 : alloc);
  }
  
  /**
   * Add a job in the appropriate pool
   */
  public synchronized void addJob(JobInProgress job) {
    getPool(getPoolName(job)).addJob(job);
  }
  
  /**
   * Add a job in the appropriate pool
   */
  public synchronized void addJob(JobInProgress job, JobInfo info) {
    getPool(getPoolName(job)).addJob(job, info);
  }
  
  /**
   * Remove a job
   */
  public synchronized void removeJob(JobInProgress job) {
    getPool(getPoolName(job)).removeJob(job);
  }
  
  /**
   * Get a collection of all pools
   */
  public synchronized Collection<YTPool> getPools() {
    return pools.values();
  }
  
  /**
   * Get the pool name for a JobInProgress from its configuration. This uses
   * the "project" property in the jobconf by default, or the property set with
   * "mapred.yunti3scheduler.poolnameproperty".
   */
  public String getPoolName(JobInProgress job) {
    JobConf conf = job.getJobConf();
    return conf.get(poolNameProperty, YTPool.DEFAULT_POOL_NAME).trim();
  }

  /**
   * Get all pool names that have been seen either in the allocation file or in
   * a MapReduce job.
   */
  public synchronized Collection<String> getPoolNames() {
    List<String> list = new ArrayList<String>();
    for (YTPool pool: getPools()) {
      list.add(pool.getName());
    }
    Collections.sort(list);
    return list;
  }

  public int getUserMaxJobs(String user) {
    if (userMaxJobs.containsKey(user)) {
      return userMaxJobs.get(user);
    } else {
      return userMaxJobsDefault;
    }
  }

  public int getPoolMaxJobs(String pool) {
    if (poolMaxJobs.containsKey(pool)) {
      return poolMaxJobs.get(pool);
    } else {
      return POOL_MAX_JOBS_DEFAULT; 
    }
  }
  
  public int getPoolWeight(String pool) {
    if (poolWeights.containsKey(pool)) {
      return poolWeights.get(pool);
    } else {
      return poolWeightDefault;
    }
  }

  public boolean getPoolUseFIFO(String pool) {
    if (poolUseFIFO.containsKey(pool)) {
      return poolUseFIFO.get(pool);
    }
    return false;
  }
  
  public ReduceWatcher getReduceWatcher(String pool) {
    return poolReduceWatcher.get(pool);
  }
    
  /**
   * Update the lass scheduled pool index to next slot. This should be called at the
   * end of a heartbeat's tasks assign.
   */
  public synchronized void updateAccessedPoolIndex() {
    if (!orderedPools.isEmpty()) {
      poolIndexLastAccessed = (poolIndexLastAccessed+1) % orderedPools.size();
    }
  }
  
  /**
   * Get the pool which will be scheduled in the coming heartbeat.
   * @return a pool if exits, else null
   */
  synchronized YTPool getCurrentScheduledPool() {
    if (!orderedPools.isEmpty()) {
      return orderedPools.get(poolIndexLastAccessed);
    } else {
      return null;
    }
  }
  
  /**
   * Create a list of ordered pools to be scheduled. According to pool's weight.
   * A pool with 2x weight has two slots. 
   * @return a list of ordered pools to be scheduled
   */
  private synchronized List<YTPool> orderPools() {
    List<YTPool> pools = new ArrayList<YTPool>();
    for (YTPool pool : getPools()) {
      Integer weight = poolWeights.get(pool.getName());
      if (weight == null)
        weight = poolWeightDefault;

      for (int i = 0; i < weight; i++) {
        pools.add(pool);
      }
    }
    return pools;
  }

}
