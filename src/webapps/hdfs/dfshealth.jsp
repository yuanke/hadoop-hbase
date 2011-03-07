<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.fs.*"
  import="org.apache.hadoop.hdfs.*"
  import="org.apache.hadoop.hdfs.server.namenode.*"
  import="org.apache.hadoop.hdfs.server.datanode.*"
  import="org.apache.hadoop.hdfs.server.common.Storage"
  import="org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory"
  import="org.apache.hadoop.hdfs.protocol.*"
  import="org.apache.hadoop.util.*"
  import="java.text.DateFormat"
  import="java.lang.Math"
  import="java.net.URLEncoder"
%>
<%!
  JspHelper jspHelper = new JspHelper();

  int rowNum = 0;
  int colNum = 0;

  String rowTxt() { colNum = 0;
      return "<tr class=\"" + (((rowNum++)%2 == 0)? "rowNormal" : "rowAlt")
          + "\"> "; }
  String colTxt() { return "<td id=\"col" + ++colNum + "\"> "; }
  void counterReset () { colNum = 0; rowNum = 0 ; }

  public void generateConfReport( JspWriter out,
                                  FSNamesystem fsn,
                                  HttpServletRequest request)
  throws IOException {
    long underReplicatedBlocks = fsn.getUnderReplicatedBlocks();
    FSImage fsImage = fsn.getFSImage();
    List<Storage.StorageDirectory> removedStorageDirs = fsImage.getRemovedStorageDirs();
    String storageDirsSizeStr="", removedStorageDirsSizeStr="", storageDirsStr="", removedStorageDirsStr="", storageDirsDiv="", removedStorageDirsDiv="";

    //FS Image storage configuration
    out.print("<h3> NameNode Storage: </h3>");
    out.print("<div id=\"dfstable\"> <table border=1 cellpadding=10 cellspacing=0 title=\"NameNode Storage\">\n"+
              "<thead><tr><td><b>Storage Directory</b></td><td><b>Type</b></td><td><b>State</b></td></tr></thead>");
      
    StorageDirectory st = null;
    for (Iterator<StorageDirectory> it = fsImage.dirIterator(); it.hasNext();) {
      st = it.next();
      String dir = "" + st.getRoot();
      String type = "" + st.getStorageDirType();
      out.print("<tr><td>"+dir+"</td><td>"+type+"</td><td>Active</td></tr>");
      }
      
    long storageDirsSize = removedStorageDirs.size();
    for(int i=0; i< storageDirsSize; i++){
      st = removedStorageDirs.get(i);
      String dir = "" + st.getRoot();
      String type = "" + st.getStorageDirType();
      out.print("<tr><td>"+dir+"</td><td>"+type+"</td><td><font color=red>Failed</font></td></tr>");
    }
      
    out.print("</table></div><br>\n");
  }
      
  public void generateDFSHealthReport(JspWriter out,
  		                                NameNode nn,
                                      HttpServletRequest request)
                                      throws IOException {
  	FSNamesystem fsn = nn.getNamesystem();
    ArrayList<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
    ArrayList<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
    jspHelper.DFSNodesStatus(live, dead);

    counterReset();
    
    long total = fsn.getCapacityTotal();
    long remaining = fsn.getCapacityRemaining();
    long used = fsn.getCapacityUsed();
    long nonDFS = fsn.getCapacityUsedNonDFS();
    float percentUsed = fsn.getCapacityUsedPercent();
    float percentRemaining = fsn.getCapacityRemainingPercent();

    out.print( "<div id=\"dfstable\"> <table>\n" +
        rowTxt() + colTxt() + "Configured Capacity" + colTxt() + ":" + colTxt() +
        FsShell.byteDesc( total ) +
        rowTxt() + colTxt() + "DFS Used" + colTxt() + ":" + colTxt() +
        FsShell.byteDesc( used ) +
        rowTxt() + colTxt() + "Non DFS Used" + colTxt() + ":" + colTxt() +
        FsShell.byteDesc( nonDFS ) +
        rowTxt() + colTxt() + "DFS Remaining" + colTxt() + ":" + colTxt() +
        FsShell.byteDesc( remaining ) +
        rowTxt() + colTxt() + "DFS Used%" + colTxt() + ":" + colTxt() +
        FsShell.limitDecimalTo2(percentUsed) + " %" +
        rowTxt() + colTxt() + "DFS Remaining%" + colTxt() + ":" + colTxt() +
        FsShell.limitDecimalTo2(percentRemaining) + " %" +
        rowTxt() + colTxt() +
        "<a href=\"dfsnodelist.jsp?whatNodes=LIVE\">Live Nodes</a> " +
        colTxt() + ":" + colTxt() + live.size() +
        rowTxt() + colTxt() +
        "<a href=\"dfsnodelist.jsp?whatNodes=DEAD\">Dead Nodes</a> " +
        colTxt() + ":" + colTxt() + dead.size() +
        "</table></div><br>\n" );
    
    if (live.isEmpty() && dead.isEmpty()) {
      out.print("There are no datanodes in the cluster");
    }
  }%>

<%
  NameNode nn = (NameNode)application.getAttribute("name.node");
  FSNamesystem fsn = nn.getNamesystem();
  String namenodeLabel = nn.getNameNodeAddress().getHostName() + ":"+
    nn.getNameNodeAddress().getPort();
%>
  
<html>

<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<title>Hadoop NameNode <%=namenodeLabel%></title>
    
<body>
<h1>NameNode '<%=namenodeLabel%>'</h1>


<div id="dfstable"> <table>	  
<tr> <td id="col1"> Started: <td> <%= fsn.getStartTime()%>
<tr> <td id="col1"> Version: <td> <%= VersionInfo.getVersion()%>, r<%= VersionInfo.getRevision()%>
<tr> <td id="col1"> Compiled: <td> <%= VersionInfo.getDate()%> by <%= VersionInfo.getUser()%>
<tr> <td id="col1"> Upgrades: <td> <%= jspHelper.getUpgradeStatusText()%>
</table></div><br>				      

<b><a href="/nn_browsedfscontent.jsp">Browse the filesystem</a></b><br>
<b><a href="/logs/">Namenode Logs</a></b>

<hr>
<h3>Cluster Summary</h3>
<b> <%= jspHelper.getSafeModeText()%> </b>
<b> <%= jspHelper.getInodeLimitText()%> </b>
<% 
    generateDFSHealthReport(out, nn, request); 
%>
<hr>
<%
  generateConfReport(out, fsn, request);
%>
<%
out.println(ServletUtil.htmlFooter());
%>
