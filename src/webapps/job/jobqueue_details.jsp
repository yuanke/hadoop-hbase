<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.util.List"
  import="java.util.ArrayList"
  import="java.util.Vector"
  import="java.util.Collection"
  import="java.util.Collections"
  import="java.util.Comparator"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.StringUtils"
  import="org.apache.hadoop.util.ServletUtil"
%>
<%!
private static final long serialVersionUID = 526456771152222127L; 
private static class JobIDComparator implements Comparator<JobInProgress> {
  public int compare(JobInProgress j1, JobInProgress j2) {
    return j1.getJobID().compareTo(j2.getJobID());
  }
};
%>
<%
  JobTracker tracker = 
    (JobTracker) application.getAttribute("job.tracker");
  String trackerName = 
    StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  String queueName = 
    StringUtils.escapeHTML(request.getParameter("queueName"));
  TaskScheduler scheduler = tracker.getTaskScheduler();
  Collection<JobInProgress> jobs = (queueName == null) ? null : scheduler.getJobs(queueName);
  if (jobs != null && jobs.size() > 1) {
    List<JobInProgress> myJobs = new ArrayList<JobInProgress>(jobs);
    Collections.sort(myJobs, new JobIDComparator());
    jobs = myJobs;
  }
  JobQueueInfo schedInfo = tracker.getQueueInfo(queueName);
  String special = request.getParameter("special");
%>
<html>
<head>
<title>Queue details for <%=queueName!=null?queueName:""%> </title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<script type="text/javascript" src="/static/jobtracker.js"></script>
</head>
<body>
<% JSPUtil.processButtons(request, response, tracker); %>
<%
  String schedulingInfoString = schedInfo.getSchedulingInfo();
%>
<h1>Hadoop Job Queue Scheduling Information on 
  <a href="jobtracker.jsp"><%=trackerName%></a>
</h1>
<div>
Scheduling Information : <%= schedulingInfoString.replaceAll("\n","<br/>") %>
</div>
<hr/>
<%
if(jobs == null || jobs.isEmpty()) {
%>
<center>
<h2> No Jobs found for the Queue :: <%=queueName!=null?queueName:""%> </h2>
<hr/>
</center>
<%
}else {
%>
<center>
<h2> Job Summary for the Queue :: <%=queueName!=null?queueName:"" %> </h2>
</center>
<div style="text-align: center;text-indent: center;font-style: italic;">
(In the order maintained by the scheduler)
</div>
<br/>
<hr/>
<%
  if (special == null) {
    out.println(JSPUtil.generateJobTable("Job List", jobs, 30, 0));
  } else {
    out.println(JSPUtil.generateSpecialJobTable("Job List", jobs, 30, 0));
  }
%>
<hr>
<% } %>

<%
out.println(ServletUtil.htmlFooter());
%>

