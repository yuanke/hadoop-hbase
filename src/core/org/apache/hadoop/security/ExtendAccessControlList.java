package org.apache.hadoop.security;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsAction;
import org.arabidopsis.ahocorasick.AhoCorasick;
import org.arabidopsis.ahocorasick.SearchResult;

public class ExtendAccessControlList {
  public static final Log LOG = LogFactory
      .getLog(ExtendAccessControlList.class);

  class RefreshACL implements Runnable {

    public RefreshACL() {
    }

    @Override
    public void run() {
      while (true) {
        try {
          UpdateACL();
          Thread.sleep(refreshTimeInterval);
        } catch (InterruptedException e) {
          LOG.fatal(e);
        } catch (IOException e) {
          LOG.error(e);
        }
      }
    }
  }

  public ExtendAccessControlList(Configuration conf) {
    refreshTimeInterval = conf.getLong("acl.refresh.interval", 3600000); // 3600000
                                                                         // means
                                                                         // default
                                                                         // is 1
                                                                         // hour
    aclFilePath = conf.get("acl.file.name");
    lastSuccessfulRefreshTime = -1;

    RefreshACL refreshAcl = new RefreshACL();
    refreshACLThread = new Thread(refreshAcl, "RefreshACL");
    refreshACLThread.start();
  }

  private void UpdateACL() throws IOException {
    if (null == aclFilePath) {
      LOG.warn("acl.file.name not defined.");
      return;
    }

    FileInputStream fis = null;
    BufferedReader br = null;
    try {
      File aclFile = new File(aclFilePath);
      long lastModified = aclFile.lastModified();

      if (lastModified == lastSuccessfulRefreshTime) {
        return;
      }

      // if acl.file.name modified less than 5 seconds, wait for 5 seconds, in
      // case the file doesn't finished.
      long currentTime = System.currentTimeMillis();
      if (currentTime - lastModified < 5000) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          LOG.error(e);
        }
      }

      fis = new FileInputStream(aclFile);
      br = new BufferedReader(new InputStreamReader(fis));

      String oneLine;
      String[] tmpStrList;
      boolean updateACL = false;
      HashMap<String, AhoCorasick> tmpUser2ACL = new HashMap<String, AhoCorasick>();

      while (null != (oneLine = br.readLine())) {
        tmpStrList = oneLine.split(":"); // user:path:permission
        if (tmpStrList.length != 3) {
          continue;
        }

        if (!tmpUser2ACL.containsKey(tmpStrList[0])) {
          tmpUser2ACL.put(tmpStrList[0], new AhoCorasick());
        }
        tmpUser2ACL.get(tmpStrList[0]).add(tmpStrList[1].getBytes(),
            tmpStrList[2]);
        updateACL = true;
      }
      for (String key : tmpUser2ACL.keySet()) {
        tmpUser2ACL.get(key).prepare();
      }

      if (updateACL) {
        user2ACL = tmpUser2ACL;
        lastSuccessfulRefreshTime = lastModified;
        LOG.info("Successfully updated extend ACL information.");
      }

    } catch (FileNotFoundException e) {
      LOG.error(e);
    } finally {
      if (null != br) {
        br.close();
      }
      if (null != fis) {
        fis.close();
      }
    }
  }

  private boolean checkPathAccess(String user, String path, FsAction access) {
    try {
      Iterator<SearchResult> searchResults = user2ACL.get(user).search(path.getBytes());

      while (searchResults.hasNext()) {
        SearchResult result = (SearchResult) searchResults.next();
        String outputs = result.getOutputs().toString();

        FsAction permission = FsAction.NONE;
        if (outputs.contains("r") || outputs.contains("R")) {
          permission = permission.or(FsAction.READ);
        }
        if (outputs.contains("w") || outputs.contains("W")) {
          permission = permission.or(FsAction.WRITE);
        }
        if (outputs.contains("x") || outputs.contains("X")) {
          permission = permission.or(FsAction.EXECUTE);
        }

        if (permission.implies(access)) {
          return true;
        }
      }

    } catch (Exception e) {
      LOG.error(e);      
    }
    return false;
  }
  
  /**
   * 
   * @param user
   * @param path
   * @param access
   *          support r w x
   * @return whether user have the permission to access the path
   */

  public boolean check(String user, String path, FsAction access) {
    if (!user2ACL.containsKey(user)) {
      return false;
    }
    
    if (checkPathAccess(user, path, access)) {
      return true;
    }
    
    // use YYYY or YYYYMMDD to indicate date pattern in path
    String newpath = path.replaceAll("/\\d{4}", "/YYYY");
    newpath = newpath.replaceAll("/YYYY\\d{4}", "/YYYYMMDD");
    
    if (path.equals(newpath)) {
      return false;
    }
    else {
      return checkPathAccess(user, newpath, access);
    }
  }

  private Long refreshTimeInterval;
  private String aclFilePath;
  private long lastSuccessfulRefreshTime;
  private Thread refreshACLThread;
  private HashMap<String, AhoCorasick> user2ACL = new HashMap<String, AhoCorasick>();
}
