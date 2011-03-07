package org.apache.hadoop.hdfs.server.namenode;

import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.jsp.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.server.datanode.*;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.util.*;
import java.text.DateFormat;
import java.lang.Math;
import java.net.URLEncoder;

public final class dfshealth_jsp extends org.apache.jasper.runtime.HttpJspBase
    implements org.apache.jasper.runtime.JspSourceDependent {


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
      out.write('\n');
      out.write('\n');

  NameNode nn = (NameNode)application.getAttribute("name.node");
  FSNamesystem fsn = nn.getNamesystem();
  String namenodeLabel = nn.getNameNodeAddress().getHostName() + ":"+
    nn.getNameNodeAddress().getPort();

      out.write("\n  \n<html>\n\n<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/hadoop.css\">\n<title>Hadoop NameNode ");
      out.print(namenodeLabel);
      out.write("</title>\n    \n<body>\n<h1>NameNode '");
      out.print(namenodeLabel);
      out.write("'</h1>\n\n\n<div id=\"dfstable\"> <table>\t  \n<tr> <td id=\"col1\"> Started: <td> ");
      out.print( fsn.getStartTime());
      out.write("\n<tr> <td id=\"col1\"> Version: <td> ");
      out.print( VersionInfo.getVersion());
      out.write(", r");
      out.print( VersionInfo.getRevision());
      out.write("\n<tr> <td id=\"col1\"> Compiled: <td> ");
      out.print( VersionInfo.getDate());
      out.write(" by ");
      out.print( VersionInfo.getUser());
      out.write("\n<tr> <td id=\"col1\"> Upgrades: <td> ");
      out.print( jspHelper.getUpgradeStatusText());
      out.write("\n</table></div><br>\t\t\t\t      \n\n<b><a href=\"/nn_browsedfscontent.jsp\">Browse the filesystem</a></b><br>\n<b><a href=\"/logs/\">Namenode Logs</a></b>\n\n<hr>\n<h3>Cluster Summary</h3>\n<b> ");
      out.print( jspHelper.getSafeModeText());
      out.write(" </b>\n<b> ");
      out.print( jspHelper.getInodeLimitText());
      out.write(" </b>\n");
 
    generateDFSHealthReport(out, nn, request); 

      out.write("\n<hr>\n");

  generateConfReport(out, fsn, request);

      out.write('\n');

out.println(ServletUtil.htmlFooter());

      out.write('\n');
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
