package org.apache.hadoop.mapred;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import javax.servlet.jsp.*;
import java.text.SimpleDateFormat;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.JobHistory.*;
import java.lang.*;

public final class jobhistory_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {


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

  private static final JspFactory _jspxFactory = JspFactory.getDefaultFactory();

  private static java.util.Vector _jspx_dependants;

  private org.apache.jasper.runtime.ResourceInjector _jspx_resourceInjector;

  public Object getDependants() {
    return _jspx_dependants;
  }

  public void _jspService(HttpServletRequest request, HttpServletResponse response)
        throws java.io.IOException, ServletException {

    PageContext pageContext = null;
    HttpSession session = null;
    ServletContext application = null;
    ServletConfig config = null;
    JspWriter out = null;
    Object page = this;
    JspWriter _jspx_out = null;
    PageContext _jspx_page_context = null;

    try {
      response.setContentType("text/html; charset=UTF-8");
      pageContext = _jspxFactory.getPageContext(this, request, response,
      			null, true, 8192, true);
      _jspx_page_context = pageContext;
      application = pageContext.getServletContext();
      config = pageContext.getServletConfig();
      session = pageContext.getSession();
      out = pageContext.getOut();
      _jspx_out = out;
      _jspx_resourceInjector = (org.apache.jasper.runtime.ResourceInjector) application.getAttribute("com.sun.appserv.jsp.resource.injector");

      out.write('\n');
  
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

      out.write("\n\n<html>\n<head>\n<title>Hadoop Map/Reduce Administration</title>\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hadoop.css\">\n</head>\n<body>\n<h1>Hadoop Map/Reduce History Viewer</h1>\n<h2><a href=\"jobtracker.jsp\">Back to JobTracker</a></h2>\n<hr>\n<form action=\"\" method=\"get\">\n<table cellpadding=3 cellspacing=2>\n  <tr>\n   <td>JobID</td>\n    <td><input id=\"JobID\" name=\"JobID\" value=\"");
      out.print(jobID);
      out.write("\" type=\"text\"></td>\n    <td>&nbsp;</td>\n   </tr>\n   <tr>\n    <td>JobName</td>\n    <td><input id=\"JobName\" name=\"JobName\" value=\"");
      out.print(jobName);
      out.write("\" type=\"text\"></td>\n    <td>&nbsp;</td>\n   </tr>\n   <tr>\n    <td>User</td>\n    <td><input id=\"UserName\" name=\"User\" value=\"");
      out.print(userName);
      out.write("\" type=\"text\"></td>\n    <td>&nbsp;</td>\n  </tr>\n  <tr>\n    <td>Group</td>\n    <td><input id=\"GroupName\" name=\"Group\" value=\"");
      out.print(groupName);
      out.write("\" type=\"text\"></td>\n    <td>&nbsp;</td>\n  </tr>\n  <tr>\n    <td>Date</td>\n    <td><input id=\"Date\" name=\"Date\" value=\"");
      out.print(date);
      out.write("\" type=\"text\"></td>\n    <td>(YY-MM-DD)&nbsp;</td>\n  </tr>\n  <tr>\n    <td>Status</td>\n    <td><input id=\"Status\" name=\"Status\"  value=\"");
      out.print(status);
      out.write("\" type=\"text\"></td>\n    <td>(Success/Failed/Killed)&nbsp;</td>\n  </tr>\n  <tr>\n    <td><INPUT type=\"submit\" value=\"Search\"></td>\n  </tr>\n</table>\n</form>\n");

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

      out.write("\n<hr/>\n");
      out.print("Results " + from + " - " + to + " of about " + RecordCount + cond);
      out.write(" \n<hr/>\n");

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

      out.write('\n');
  
    printJob(tmpJobSubmitTime, tmpJobId,
      tmpJobName, tmpUser, tmpGroup, tmpStatus, new Path(historyLogDir, encodedJobFileName),out) ;
  }
  out.print("<table/>");

      out.write("\n<hr>\n");

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

      out.write('\n');
      out.write('\n');
      out.write(" \n</body></html>\n");
    } catch (Throwable t) {
      if (!(t instanceof SkipPageException)){
        out = _jspx_out;
        if (out != null && out.getBufferSize() != 0)
          out.clearBuffer();
        if (_jspx_page_context != null) _jspx_page_context.handlePageException(t);
      }
    } finally {
      _jspxFactory.releasePageContext(_jspx_page_context);
    }
  }
}
