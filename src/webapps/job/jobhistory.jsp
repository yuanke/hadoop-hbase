<%@ page
  contentType="text/html; charset=UTF-8"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.util.*"
  import="org.apache.hadoop.fs.*"
  import="javax.servlet.jsp.*"
  import="java.text.SimpleDateFormat"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.mapred.JobHistory.*"
  import="java.lang.*"
%>
<%  
  int PageSize = 20;
  int CounterSize = 11;
  int StartRow = 0;
  int EndRow = 0;
  int PageNo=0;
  int CounterStart=0;
  int CounterEnd=0;
  int RecordCount=0;
  int MaxPage=0;
  int PrevStart=0;
  int NextPage=0;
  List<HistoryIndexManager.Document> jobFiles = null;
  String userName = null;
  String groupName = null;
  String date = null;
  String status = null;
  String jobID = null;
  String jobName = null;
  
  String historyLogDir = (String) application.getAttribute("historyLogDir");  
  
  jobID = request.getParameter("JobID");
  if(jobID == null)
    jobID = "";
  jobName = request.getParameter("JobName");
  if(jobName == null)
    jobName = "";
    
  userName = request.getParameter("User");
  if(userName == null)
    userName = "";
  groupName = request.getParameter("Group");
  if(groupName == null)
    groupName = "";
  date = request.getParameter("Date");
  if(date == null)
    date = "";
  status = request.getParameter("Status");
  if(status == null)
    status = "";

  jobFiles = JobHistory.him.search(userName, groupName, date, status, jobID, jobName);
  RecordCount = jobFiles.size();

  if(RecordCount % PageSize == 0)
    MaxPage = RecordCount / PageSize;
  else
    MaxPage = RecordCount / PageSize + 1;

  String pn = request.getParameter("PageNo");
  if(pn == null || pn.isEmpty())
    PageNo = 1;
  else
  {
    try
    {
      PageNo = Integer.parseInt(pn);
    }
    catch(Exception e)
    {
      PageNo = 1;  
    }
  }
  
  if(PageNo <= 0 || PageNo > MaxPage)
    PageNo = 1;  

  StartRow = (PageNo - 1) * PageSize;
        
  
    if(PageNo > CounterSize/2)
      CounterStart = PageNo - (CounterSize/2);
    else
      CounterStart = 1;
      
    CounterEnd = CounterStart + CounterSize - 1;
    
    if(CounterEnd > MaxPage){
      CounterEnd = MaxPage;
      CounterStart = CounterEnd - CounterSize + 1;
      if(CounterStart < 1)
        CounterStart = 1;
    }
    
    EndRow= StartRow+PageSize;
    if(EndRow > RecordCount)EndRow = RecordCount;
%>

<html>
<head>
<title>Hadoop Map/Reduce Administration</title>
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
</head>
<body>
<h1>Hadoop Map/Reduce History Viewer</h1>
<h2><a href="jobtracker.jsp">Back to JobTracker</a></h2>
<hr>
<form action="" method="get">
<table cellpadding=3 cellspacing=2>
  <tr>
   <td>JobID</td>
    <td><input id="JobID" name="JobID" value="<%=jobID%>" type="text"></td>
    <td>&nbsp;</td>
   </tr>
   <tr>
    <td>JobName</td>
    <td><input id="JobName" name="JobName" value="<%=jobName%>" type="text"></td>
    <td>&nbsp;</td>
   </tr>
   <tr>
    <td>User</td>
    <td><input id="UserName" name="User" value="<%=userName%>" type="text"></td>
    <td>&nbsp;</td>
  </tr>
  <tr>
    <td>Group</td>
    <td><input id="GroupName" name="Group" value="<%=groupName%>" type="text"></td>
    <td>&nbsp;</td>
  </tr>
  <tr>
    <td>Date</td>
    <td><input id="Date" name="Date" value="<%=date%>" type="text"></td>
    <td>(YY-MM-DD)&nbsp;</td>
  </tr>
  <tr>
    <td>Status</td>
    <td><input id="Status" name="Status"  value="<%=status%>" type="text"></td>
    <td>(Success/Failed/Killed)&nbsp;</td>
  </tr>
  <tr>
    <td><INPUT type="submit" value="Search"></td>
  </tr>
</table>
</form>
<%
  String cond = " for";
  if(jobID != null && !jobID.isEmpty())
    cond += " jobID=\"" + jobID +"\"";
  if(jobName != null && !jobName.isEmpty())
    cond += " jobName=\"" + jobName +"\"";
  if(userName != null && !userName.isEmpty())
     cond += " user=\"" + userName + "\"";
  if(groupName != null && !groupName.isEmpty())
     cond += " group=\"" + groupName + "\"";
  if(date != null && !date.isEmpty())
     cond += " date=\"" + date + "\"";
  if(status != null && !status.isEmpty())
     cond += " status=\"" + status + "\"";
    
  
  int from = StartRow + 1;
  int to = EndRow;
  
  jobFiles = jobFiles.subList(StartRow, EndRow);
%>
<hr/>
<%="Results " + from + " - " + to + " of about " + RecordCount + cond%> 
<hr/>
<%
  out.print("<table border=2 cellpadding=\"5\" cellspacing=\"2\">");    
      out.print("<tr>");
      out.print(
                "<td>Submit time</td>" +
                "<td>Job Id</td><td>Name</td><td>User</td>" +
                "<td>Group</td>" +
                "<td>Status</td>") ; 
      out.print("</tr>");
  
  if(jobFiles == null)return;
  
  for(int start=0;start<jobFiles.size();start++){
    HistoryIndexManager.Document doc = jobFiles.get(start);
    String tmpJobID = doc.jobID;
    String decodedJobFileName = JobHistory.JobInfo.decodeJobHistoryFileName(tmpJobID);
  
    String[] jobDetails = decodedJobFileName.split("_");
    String tmpJobSubmitTime = doc.date;
    String tmpJobId = jobDetails[2] + "_" +jobDetails[3] + "_" + jobDetails[4] ;
    String tmpUser = doc.user;
    String tmpJobName = jobDetails[6];
    String tmpGroup = doc.group;
    String tmpStatus = doc.status;
    for(int k=7;k<jobDetails.length;k++)
      tmpJobName+="_"+jobDetails[k];
    
    String encodedJobFileName =              
      JobHistory.JobInfo.encodeJobHistoryFileName(tmpJobID);
%>
<%  
    printJob(tmpJobSubmitTime, tmpJobId,
      tmpJobName, tmpUser, tmpGroup, tmpStatus, new Path(historyLogDir, encodedJobFileName),out) ;
  }
  out.print("<table/>");
%>
<hr>
<%
   String parameter = "";
  parameter+="&User="+userName+"&Group="+groupName+"&Date="+date+"&Status="+status +"&JobID="+jobID+"&JobName="+jobName;
  parameter = parameter.replaceAll(" ", "\\+");
   
  if(PageNo != 1){
    PrevStart = PageNo - 1;
    out.print("<a href=jobhistory.jsp?PageNo=1"+parameter+">First</a> &nbsp;");
    out.print("<a href=jobhistory.jsp?PageNo="+PrevStart+parameter+">Pre</a> &nbsp;");
  }

   for(int c=CounterStart;c<=CounterEnd;c++){
   if(c <CounterEnd){
     if(c == PageNo){
          out.print(c+"&nbsp;");
     }else{
        out.print("<a href=jobhistory.jsp?PageNo="+c+parameter+">"+c+"</a>&nbsp;");
     }
   }else{
     if(PageNo == MaxPage){
      out.print(c);
      break;
     }else{
        out.print("<a href=jobhistory.jsp?PageNo="+c+parameter+">"+c+"</a>&nbsp;");
     break;
   }
  }
}

if(PageNo < MaxPage){
    NextPage = PageNo + 1;
    out.print("<a href=jobhistory.jsp?PageNo="+NextPage+parameter+">Next</a>&nbsp;");
}

if(PageNo < MaxPage){
    out.print("<a href=jobhistory.jsp?PageNo="+MaxPage+parameter+">Last</a>");
  }
%>

<%!
    private void printJob(String jobSubmitTime,
                          String jobId, String jobName,
                          String user, String group, String status,
                          Path logFile, JspWriter out)
    throws IOException {
      out.print("<tr>"); 
      out.print("<td>" + jobSubmitTime + "</td>"); 
      out.print("<td>" + "<a href=\"jobdetailshistory.jsp?jobid=" + jobId + 
                "&logFile=" + logFile.toString() + "\">" + jobId + "</a></td>"); 
      out.print("<td>" + jobName + "</td>"); 
      out.print("<td>" + user + "</td>");
      out.print("<td>" + group + "</td>");
      out.print("<td>" + status + "</td>");  
      out.print("</tr>");
    }
%> 
</body></html>
