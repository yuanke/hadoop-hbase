package org.apache.hadoop.security;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MD5Hash;

public class UserPasswordInformation {

  public static final Log LOG = LogFactory
      .getLog(UserPasswordInformation.class);

  class RefreshUsers implements Runnable {
    public RefreshUsers() {
    }

    public void run() {
      while (true) {
        try {
          readPasswordFromFile();
          Thread.sleep(refreshTimeInterval);
        } catch (InterruptedException e) {
          break;
        } catch (IOException e) {
          LOG.error(e);
        }
      }
    }
  }

  public UserPasswordInformation(Configuration conf) {
    passwdFilePath = conf.get("user.password.file.name");
    refreshTimeInterval = conf.getLong("user.password.interval", 300000);
    lastSuccessfulRefreshTime = -1;

    RefreshUsers refreshUsers = new RefreshUsers();
    refreshUsersThread = new Thread(refreshUsers, "refreshUsers");
    refreshUsersThread.start();
  }

  public int readPasswordFromFile() throws IOException {
    if (null == passwdFilePath) {
      LOG.warn("user.password.file.name not defined.");
      return -1;
    }

    FileInputStream fis = null;
    BufferedReader br = null;
    try {

      File passwdFile = new File(passwdFilePath);
      long lastModified = passwdFile.lastModified();

      if (lastModified == lastSuccessfulRefreshTime) {
        return 0;
      }

      // if user.password.file.name modified less than 5 seconds, wait for 5
      // seconds, in case the file doesn't finished.
      long currentTime = System.currentTimeMillis();
      if (currentTime - lastModified < 5000) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          LOG.fatal(e);
        }
      }

      fis = new FileInputStream(passwdFile);
      br = new BufferedReader(new InputStreamReader(fis));

      HashMap<String, String> tmpUser2Passwd = new HashMap<String, String>();
      HashMap<String, HashSet<String>> tmpUser2Groups = new HashMap<String, HashSet<String>>();

      String strLine;
      String[] userPassGroups;

      int cntUser = 0;
      while ((strLine = br.readLine()) != null) {
        userPassGroups = strLine.split(":"); // user:password:group1[:groupN...]

        if (userPassGroups.length < 2) {
          LOG.error("User, password and group information not complete: "
              + strLine);
          continue;
        }

        tmpUser2Passwd.put(userPassGroups[0], userPassGroups[1]);

        HashSet<String> groupsSet = new HashSet<String>();
        for (int idxUPG = 2; idxUPG < userPassGroups.length; idxUPG++) {
          groupsSet.add(userPassGroups[idxUPG]);
        }
        tmpUser2Groups.put(userPassGroups[0], groupsSet);

        cntUser += 1;
      }

      if (cntUser > 0) {
        user2Passwd = tmpUser2Passwd;
        user2Groups = tmpUser2Groups;
        lastSuccessfulRefreshTime = lastModified;
        // if user checkGroup exists and password is "true", then check group will be enable
        if (user2Passwd.containsKey("checkGroup")
            && MD5_OF_TRUE.equals(user2Passwd
                .get("checkGroup"))) {
          checkGroup = true;
          LOG
              .info("Check group enabled. Set special user checkGroup's password other from true can disable it.");
        } else {
          checkGroup = false;
          LOG
              .info("Check group disabled. Set special user checkGroup's password to true can enable it.");
        }

        LOG.info("Successfully updated user and password information.");
      }
    } catch (FileNotFoundException e) {
      LOG.fatal(e);
      return -1;
    } catch (IOException e) {
      LOG.fatal(e);
      return -1;
    } finally {
      if (null != br) {
        br.close();
      }
      if (null != fis) {
        fis.close();
      }
    }
    return 0;
  }

  public boolean checkUserAndPassword(String user, String passwd,
      String[] groups) {
    if (null == passwdFilePath) {
      return true;
    }

    if (!user2Passwd.containsKey(user)) {
      return false;
    }

    if (checkGroup) {
      HashSet<String> groupsSet = user2Groups.get(user);
      for (String group : groups) {
        if (!groupsSet.contains(group)) {
          if (!group.startsWith("cug-")) {
            continue;
          }
          LOG.info(group + " not in " + groupsSet);
          return false;
        }
      }
    }

    if ("".equals(passwd)) {
      if ("".equals(user2Passwd.get(user))) {
        return true;
      } else {
        return false;
      }
    } else {
      MD5Hash md5hs = MD5Hash.digest(passwd);

      if (!md5hs.toString().equals(user2Passwd.get(user))) {
        return false;
      }
      return true;
    }
  }

  private HashMap<String, String> user2Passwd = new HashMap<String, String>();
  private HashMap<String, HashSet<String>> user2Groups = new HashMap<String, HashSet<String>>();
  private String passwdFilePath;
  private Long refreshTimeInterval;
  private Thread refreshUsersThread;
  private long lastSuccessfulRefreshTime;
  private boolean checkGroup = false;
  private final String MD5_OF_TRUE = "b326b5062b2f0e69046810717534cb09"; 
}
