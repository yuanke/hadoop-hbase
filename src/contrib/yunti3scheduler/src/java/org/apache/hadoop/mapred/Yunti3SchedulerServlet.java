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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.mapred.Yunti3Scheduler.JobInfo;
import org.apache.hadoop.util.StringUtils;

/**
 * Servlet for displaying fair scheduler information, installed at
 * [job tracker URL]/scheduler when the {@link Yunti3Scheduler} is in use.
 * 
 * The main features are viewing each job's task count and fair share, ability
 * to change job priorities and pools from the UI, and ability to switch the
 * scheduler to FIFO mode without restarting the JobTracker if this is required
 * for any reason.
 * 
 * There is also an "advanced" view for debugging that can be turned on by
 * going to [job tracker URL]/scheduler?advanced.
 */
public class Yunti3SchedulerServlet extends HttpServlet {
  private static final long serialVersionUID = 9104070533067306659L;
  private static final DateFormat DATE_FORMAT = 
    new SimpleDateFormat("MMM dd, HH:mm");
  
  private Yunti3Scheduler scheduler;
  private JobTracker jobTracker;
  private static long lastId = 0; // Used to generate unique element IDs
  private static final String[] jobLevelIncrements = new String[] {
    "+100", "+50", "+20", "+10", "+9", "+8", "+7", "+6", "+5", "+4", "+3", "+2", "+1", "0",
    "-1", "-2", "-3", "-4", "-5", "-6", "-7", "-8", "-9", "-10", "-20", "-50", "-100"};

  @Override
  public void init() throws ServletException {
    super.init();
    ServletContext servletContext = this.getServletContext();
    this.scheduler = (Yunti3Scheduler) servletContext.getAttribute("scheduler");
    this.jobTracker = (JobTracker) scheduler.taskTrackerManager;
  }
  
  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    doGet(req, resp); // Same handler for both GET and POST
  }
  
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    // If the request has a set* param, handle that and redirect to the regular
    // view page so that the user won't resubmit the data if they hit refresh.
    boolean advancedView = request.getParameter("advanced") != null;

    if (request.getParameter("incrJobLevel") != null) {
      String incr = request.getParameter("incrJobLevel");
      String jobId = request.getParameter("jobid");
      String poolName = request.getParameter("pool");

      synchronized (scheduler) {
        YTPoolManager poolMgr = scheduler.getPoolManager();
        YTPool pool = poolMgr.getPool(poolName);
        pool.updateJobLevel(jobId, incr);
      }
 
      response.sendRedirect("/scheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    
    // Print out the normal response
    response.setContentType("text/html");
    
    // Because the client may read arbitrarily slow, and we hould locks while
    // the servlet output, we want to write to our own buffer which we know
    // won't block.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter out = new PrintWriter(baos);
    String hostname = StringUtils.simpleHostname(
        jobTracker.getJobTrackerMachine());
    out.print("<html><head>");
    out.printf("<title>%s Job Scheduler Admininstration</title>\n", hostname);
    out.print("<link rel=\"stylesheet\" type=\"text/css\" " + 
        "href=\"/static/hadoop.css\">\n");
    out.print("</head><body>\n");
    out.printf("<h1><a href=\"/jobtracker.jsp\">%s</a> " + 
        "Job Scheduler Administration</h1>\n", hostname);
    showPools(out, advancedView);
    showJobs(out,advancedView);
    out.print("</body></html>\n");
    out.close();
    
    // Flush our buffer to the real servlet output
    OutputStream servletOut = response.getOutputStream();
    baos.writeTo(servletOut);
    servletOut.close();
  }

  /**
   * Print a view of pools to the given output writer.
   */
  private void showPools(PrintWriter out, boolean advancedView) {
    synchronized(scheduler) {
      YTPoolManager poolManager = scheduler.getPoolManager();
      out.print("<h2>Pools</h2>\n");
      out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n");
      out.print("<tr><th>Pool</th><th>Running Jobs</th>" + 
          "<th>Max Running Jobs</th>" +          
          "<th>Min Maps</th><th>Min Reduces</th>" + 
          "<th>Running Maps</th><th>Running Reduces</th><th>Weight</th></tr>\n");
      List<YTPool> pools = new ArrayList<YTPool>(poolManager.getPools());
      Collections.sort(pools, new Comparator<YTPool>() {
        public int compare(YTPool p1, YTPool p2) {
          return p1.getName().compareTo(p2.getName());
        }});
      for (YTPool pool: pools) {
        out.print("<tr>\n");
        out.printf("<td><a href=\"#%s\">%s</a></td>\n", pool.getName().replace(' ', '_'), 
                                                        pool.getName());
        out.printf("<td>%s</td>\n", pool.getJobs().size());
        out.printf("<td>%s</td>\n", poolManager.getPoolMaxJobs(pool.getName()));
        out.printf("<td>%s</td>\n", poolManager.getAllocation(pool.getName(),
            YTTaskType.MAP));
        out.printf("<td>%s</td>\n", poolManager.getAllocation(pool.getName(), 
            YTTaskType.REDUCE));
        out.printf("<td>%s</td>\n", pool.getTotalRunningMaps());
        out.printf("<td>%s</td>\n", pool.getTotalRunningReduces());
        out.printf("<td>%s</td>\n", poolManager.getPoolWeight(pool.getName()));
        out.print("</tr>\n");
      }
      out.print("</table>\n");
    }
  }

  /**
   * Grouping all the running jobs from JobTracker and show them in separated tables 
   * according to Pool.
   */
  private void showJobs(PrintWriter out, boolean advancedView) {
    Map<YTPool, List<JobInProgress>> poolRunningJobs = new HashMap<YTPool, List<JobInProgress>>();

    synchronized(jobTracker) {
      List<JobInProgress> runningJobs = jobTracker.getRunningJobs();
      synchronized(scheduler) {
        YTPoolManager poolMgr = scheduler.getPoolManager();
        for (JobInProgress job : runningJobs) {
          YTPool pool = poolMgr.getPool(poolMgr.getPoolName(job));

          if (poolRunningJobs.containsKey(pool)) {
            poolRunningJobs.get(pool).add(job);
          } else {
            List<JobInProgress> jobs = new ArrayList<JobInProgress>();
            jobs.add(job);
            poolRunningJobs.put(pool, jobs);
          }
        }
      }
    }

    out.print("<h2>Running Jobs</h2>\n");
    for (Entry<YTPool, List<JobInProgress>> entry : poolRunningJobs.entrySet()) {
      showJobs(out, advancedView, entry.getKey(), entry.getValue());
    }
  }
  
  
  /**
   * Print a view of running jobs to the given output writer.
   */
  private void showJobs(PrintWriter out, boolean advancedView, YTPool pool, List<JobInProgress> jobs) {
    out.printf("<br><b id=\"%s\">Pool: %s</b> ", pool.getName().replace(' ', '_'), pool.getName());
    showScheduleInfo(out, pool.getName());
    out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n");
    int colsPerTaskType = advancedView ? 4 : 3;
    out.printf("<tr><th rowspan=2>Submitted</th>" + 
        "<th rowspan=2>JobID</th>" +
        "<th rowspan=2>User</th>" +
        "<th rowspan=2>Name</th>" +
        "<th rowspan=2>Level</th>" +
        "<th rowspan=2>Priority</th>" +
        "<th colspan=%d>Maps</th>" +
        "<th colspan=%d>Reduces</th>",
        colsPerTaskType, colsPerTaskType);
    out.print("</tr><tr>\n");
    out.print("<th>Finished</th><th>Running</th><th>Fair Share</th>" +
        (advancedView ? "<th>Weight</th>" : ""));
    out.print("<th>Finished</th><th>Running</th><th>Fair Share</th>" +
        (advancedView ? "<th>Weight</th>" : ""));
    out.print("</tr>\n");
    for (JobInProgress job: jobs) {
      JobProfile profile = job.getProfile();
      JobInfo info = pool.getJobInfo(job);      
      if (info == null) { // Job finished, but let's show 0's for info
        info = new JobInfo();
      }
      out.print("<tr>\n");
      out.printf("<td>%s</td>\n", DATE_FORMAT.format(
          new Date(job.getStartTime())));
      out.printf("<td><a href=\"jobdetails.jsp?jobid=%s\">%s</a></td>",
          profile.getJobID(), profile.getJobID());
      out.printf("<td>%s</td>\n", profile.getUser());
      out.printf("<td>%s</td>\n", profile.getJobName());
      if (advancedView) {
        out.printf("<td>%s</td>\n", generateJobLevelSelect(
            info.jobLevel,
            "/scheduler?incrJobLevel=<CHOICE>&pool=" + pool.getName() + 
            "&jobid=" + profile.getJobID() + "&advanced"));
      } else {
        out.printf("<td>%d</td>\n", info.jobLevel);
      }
      out.printf("<td>%s</td>\n", job.getPriority().toString());
      out.printf("<td>%d / %d</td><td>%d</td><td>%8.1f</td>\n",
          job.finishedMaps(), job.desiredMaps(), info.runningMaps,
          info.mapFairShare);
      if (advancedView) {
        out.printf("<td>%8.1f</td>\n", info.mapWeight);        
      }
      out.printf("<td>%d / %d</td><td>%d</td><td>%8.1f</td>\n",
          job.finishedReduces(), job.desiredReduces(), info.runningReduces,
          info.reduceFairShare);
      if (advancedView) {
        out.printf("<td>%8.1f</td>\n", info.reduceWeight);        
      }
      out.print("</tr>\n");
    }
    out.print("</table>\n");
  }
  
  /**
   * Generate a HTML select control with a given list of job levels and a given
   * level selected. When the selection is changed, take the user to the
   * <code>submitUrl</code>. The <code>submitUrl</code> can be made to include
   * the option selected -- the first occurrence of the substring
   * <code>&lt;CHOICE&gt;</code> will be replaced by the option chosen.
   */
  private String generateJobLevelSelect(int myLevel, String submitUrl) {
    StringBuilder html = new StringBuilder();
    String id = "select" + lastId++;
    html.append("<select id=\"" + id + "\" name=\"" + id + "\" " + 
        "onchange=\"window.location = '" + submitUrl + 
        "'.replace('<CHOICE>', document.getElementById('" + id +
        "').value);\">\n");
    
    String level = Integer.toString(myLevel);
    for (String incr : jobLevelIncrements) {
      html.append(String.format("<option value=\"%s\"%s>%s</option>\n",
          (incr.equals("0") ? level : incr), 
          (incr.equals("0") ? " selected" : ""), 
          (incr.equals("0") ? level : incr)));
    }
    html.append("</select>\n");
    return html.toString();
  }

  /**
   * Print a view of scheduling information to the given output writer,
   * which is the same with jobtracker.jsp's Scheduling Information.
   * @param out
   * @param poolName pool's name
   */
  private void showScheduleInfo(PrintWriter out, String poolName) {
    JobQueueInfo info = null;
    try {
      info = jobTracker.getQueueInfo(poolName);
    } catch (IOException e) {
      info = null;
    }
    String sinfo = (info == null) ? null : info.getSchedulingInfo();
    if(sinfo == null || sinfo.trim().equals("")) {
      sinfo = "N/A";
    }
    out.printf("(%s)<br>", sinfo);
  }
  
}
