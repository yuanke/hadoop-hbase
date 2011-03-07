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
import java.text.DecimalFormat;
import java.text.Format;
import java.util.Iterator;
import java.util.Collection;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ServletUtil;

class JSPUtil {
  private static final String PRIVATE_ACTIONS_KEY = "webinterface.private.actions";
  
  public static final Configuration conf = new Configuration();

  /**
   * Method used to process the request from the job page based on the 
   * request which it has received. For example like changing priority.
   * 
   * @param request HTTP request Object.
   * @param response HTTP response object.
   * @param tracker {@link JobTracker} instance
   * @throws IOException
   */
  public static void processButtons(HttpServletRequest request,
      HttpServletResponse response, JobTracker tracker) throws IOException {

    if (conf.getBoolean(PRIVATE_ACTIONS_KEY, false)
        && request.getParameter("killJobs") != null) {
      String[] jobs = request.getParameterValues("jobCheckBox");
      if (jobs != null) {
        for (String job : jobs) {
          tracker.killJob(JobID.forName(job));
        }
      }
    }

    if (conf.getBoolean(PRIVATE_ACTIONS_KEY, false) && 
          request.getParameter("changeJobPriority") != null) {
      String[] jobs = request.getParameterValues("jobCheckBox");

      if (jobs != null) {
        JobPriority jobPri = JobPriority.valueOf(request
            .getParameter("setJobPriority"));

        for (String job : jobs) {
          tracker.setJobPriority(JobID.forName(job), jobPri);
        }
      }
    }
  }

  /**
   * Method used to generate the Job table for Job pages.
   * 
   * @param label display heading to be used in the job table.
   * @param jobs vector of jobs to be displayed in table.
   * @param refresh refresh interval to be used in jobdetails page.
   * @param rowId beginning row id to be used in the table.
   * @return
   * @throws IOException
   */
  public static String generateJobTable(String label, Collection<JobInProgress> jobs
      , int refresh, int rowId) throws IOException {
	  return generateJobAdminTable(label, jobs, refresh, rowId, false);
  }

  /**
   * Method used to generate the Job table for Job pages. We add some counter fields
   * for administrator. --added by liangly.
   * @param label display heading to be used in the job table.
   * @param jobs vector of jobs to be displayed in table.
   * @param refresh refresh interval to be used in jobdetails page.
   * @param rowId beginning row id to be used in the table.
   * @param advanced display File Systems' counters or not.
   * @return
   * @throws IOException
   */
  public static String generateJobAdminTable(String label, Collection<JobInProgress> jobs
      , int refresh, int rowId, boolean advanced) throws IOException {
    return generateJobAdminTable(label, jobs, refresh, rowId, advanced, false);
  }
  
  /**
   * Method used to generate the Job table for Job pages. Add special
   * fields for customers. 
   * @param label display heading to be used in the job table.
   * @param jobs vector of jobs to be displayed in table.
   * @param refresh refresh interval to be used in jobdetails page.
   * @param rowId beginning row id to be used in the table.
   * @param special show job level and skynet id, instead of priority and pool
   * @return
   * @throws IOException
   */
  public static String generateSpecialJobTable(String label, Collection<JobInProgress> jobs
      , int refresh, int rowId) throws IOException {
    return generateJobAdminTable(label, jobs, refresh, rowId, false, true);
  }
  
  /**
   * Method used to generate the Job table for Job pages. We add some counter fields
   * for administrator and special fields for customers. 
   * @param label display heading to be used in the job table.
   * @param jobs vector of jobs to be displayed in table.
   * @param refresh refresh interval to be used in jobdetails page.
   * @param rowId beginning row id to be used in the table.
   * @param advanced display File Systems' counters or not.
   * @param special show job level and skynet id, instead of priority and pool
   * @return
   * @throws IOException
   */
  public static String generateJobAdminTable(String label, Collection<JobInProgress> jobs
      , int refresh, int rowId, boolean advanced, boolean special) throws IOException {

    boolean isModifiable = label.equals("Running") 
                                && conf.getBoolean(
                                      PRIVATE_ACTIONS_KEY, false);
    long io_threshold = conf.getLong("webinterface.job.io.threshold", 1099511627776L); // default 1T

    StringBuffer sb = new StringBuffer();

    sb.append("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n");

    if (jobs.size() > 0) {
      if (isModifiable) {
        sb.append("<form action=\"/jobtracker.jsp\" onsubmit=\"return confirmAction();\" method=\"POST\">");
        sb.append("<tr>");
        sb.append("<td><input type=\"Button\" onclick=\"selectAll()\" " +
        		"value=\"Select All\" id=\"checkEm\"></td>");
        sb.append("<td>");
        sb.append("<input type=\"submit\" name=\"killJobs\" value=\"Kill Selected Jobs\">");
        sb.append("</td");
        sb.append("<td><nobr>");
        sb.append("<select name=\"setJobPriority\">");

        for (JobPriority prio : JobPriority.values()) {
          sb.append("<option"
              + (JobPriority.NORMAL == prio ? " selected=\"selected\">" : ">")
              + prio + "</option>");
        }

        sb.append("</select>");
        sb.append("<input type=\"submit\" name=\"changeJobPriority\" " +
        		"value=\"Change\">");
        sb.append("</nobr></td>");
        sb.append("<td colspan=\"10\">&nbsp;</td>");
        sb.append("</tr>");
        sb.append("<td>&nbsp;</td>");
      } else {
        sb.append("<tr>");
      }
      if (advanced) {
    	  sb.append("<th rowspan=2><b>Jobid</b></th>");
    	  sb.append("<th rowspan=2><b>Priority</b></th>");
    	  sb.append("<th rowspan=2><b>User</b></th>");
    	  sb.append("<th rowspan=2><b>Pool</b></th>");
    	  sb.append("<th rowspan=2><b>Name</b></th>");
    	  sb.append("<th rowspan=2><b>Map % Complete</b></th>");
    	  sb.append("<th rowspan=2><b>Map Total</b></th>");
    	  sb.append("<th rowspan=2><b>Maps Completed</b></th>");
    	  sb.append("<th rowspan=2><b>Reduce % Complete</b></th>");
    	  sb.append("<th rowspan=2><b>Reduce Total</b></th>");
    	  sb.append("<th rowspan=2><b>Reduces Completed</b></th>");

    	  sb.append("<th colspan=4><b>File Systems</b></th>");
    	  sb.append("</tr>\n<tr>");
    	  sb.append("<th>HDFS bytes read</th><th>HDFS bytes written</th><th>Local bytes read</th><th>Local bytes written</th>");
      }  else {
    	  sb.append("<td><b>Jobid</b></td>");
    	  if (special) {
    	    sb.append("<td><b>Level</b></td>");
    	  } else {
    	    sb.append("<td><b>Priority</b></td>");
    	  }
    	  sb.append("<td><b>User</b></td>");
    	  if (special) {
    	    sb.append("<td><b>SkynetID</b></td>");
    	  } else {
    	    sb.append("<td><b>Pool</b></td>");
    	  }
    	  sb.append("<td><b>Name</b></td>");
    	  sb.append("<td><b>Map % Complete</b></td>");
    	  sb.append("<td><b>Map Total</b></td>");
    	  sb.append("<td><b>Maps Completed</b></td>");
    	  sb.append("<td><b>Reduce % Complete</b></td>");
    	  sb.append("<td><b>Reduce Total</b></td>");
    	  sb.append("<td><b>Reduces Completed</b></td>");
      }
      sb.append("</tr>\n");

      for (Iterator<JobInProgress> it = jobs.iterator(); it.hasNext(); ++rowId) {
        JobInProgress job = it.next();
        JobProfile profile = job.getProfile();
        JobStatus status = job.getStatus();
        JobID jobid = profile.getJobID();

        int desiredMaps = job.desiredMaps();
        int desiredReduces = job.desiredReduces();
        int completedMaps = job.finishedMaps();
        int completedReduces = job.finishedReduces();
        String name = profile.getJobName();
        String jobpri = job.getPriority().toString();
        String jobLevel = job.getJobConf().get("mapred.job.level", "0");
        String skynetID = job.getJobConf().get("mapred.job.skynet_id", "N/A");
        
        // default 0, meaningless
        String hdfs_bytes_read = "0";
        String hdfs_bytes_written = "0";
        String local_bytes_read = "0";
        String local_bytes_written = "0";
        
        boolean hightlight = false;
        if (advanced) {
        	// get total counter about File Systems
        	Counters totalCounters = job.getCounters();
        	Counters.Group totalGroup = totalCounters.getGroup("org.apache.hadoop.mapred.Task$FileSystemCounter");

        	Format decimal = new DecimalFormat(); // a decimal fromat
        	hdfs_bytes_read = decimal.format(totalGroup.getCounter("HDFS bytes read"));
        	hdfs_bytes_written = decimal.format(totalGroup.getCounter("HDFS bytes written"));
        	local_bytes_read = decimal.format(totalGroup.getCounter("Local bytes read"));
        	local_bytes_written = decimal.format(totalGroup.getCounter("Local bytes written"));
        	
        	if (totalGroup.getCounter("HDFS bytes read") > io_threshold 
        	    || totalGroup.getCounter("HDFS bytes written") > io_threshold
        	    || totalGroup.getCounter("Local bytes read") > io_threshold
        	    || totalGroup.getCounter("Local bytes written") > io_threshold)
        	  hightlight = true;
        }
        if (hightlight)
          sb.append("<tr style=\"color:#FF0000; font-weight:bold\">");
        else
          sb.append("<tr>");
        
        if (isModifiable) {
          sb.append("<td><input TYPE=\"checkbox\" " +
          		"onclick=\"checkButtonVerbage()\" " +
          		"name=\"jobCheckBox\" value="
                  + jobid + "></td>");
        }

        sb.append("<td id=\"job_" + rowId
            + "\"><a href=\"jobdetails.jsp?jobid=" + jobid + "&refresh="
            + refresh + "\">" + jobid + "</a></td>" + "<td id=\"priority_"
            + rowId + "\">" + (special ? jobLevel : jobpri) + "</td>" 
            + "<td id=\"user_" + rowId + "\">" + profile.getUser() + "</td>" 
            + "<td id=\"pool_" + rowId + "\">" + (special ? skynetID : profile.getQueueName()) 
            + "</td><td id=\"name_" + rowId
            + "\">" + ("".equals(name) ? "&nbsp;" : name) + "</td>" + "<td>"
            + StringUtils.formatPercent(status.mapProgress(), 2)
            + ServletUtil.percentageGraph(status.mapProgress() * 100, 80)
            + "</td><td>" + desiredMaps + "</td><td>" + completedMaps
            + "</td><td>"
            + StringUtils.formatPercent(status.reduceProgress(), 2)
            + ServletUtil.percentageGraph(status.reduceProgress() * 100, 80)
            + "</td><td>" + desiredReduces + "</td><td> " + completedReduces + "</td>");
        if (advanced) {
          sb.append("<td>" + hdfs_bytes_read + "</td>"
              + "<td>" + hdfs_bytes_written + "</td>"
              + "<td>" + local_bytes_read + "</td>"
              + "<td>" + local_bytes_written + "</td>");
        }

        sb.append("</tr>\n");

      }
      if (isModifiable) {
        sb.append("</form>\n");
      }
    } else {
      sb.append("<tr><td align=\"center\" colspan=\"8\"><i>none</i>" +
      		"</td></tr>\n");
    }
    sb.append("</table>\n");
    
    return sb.toString();
  }
}