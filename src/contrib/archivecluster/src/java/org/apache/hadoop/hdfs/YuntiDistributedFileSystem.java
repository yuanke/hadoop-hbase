package org.apache.hadoop.hdfs;

import java.io.*;
import java.lang.reflect.Field;
import java.net.*;

import javax.net.SocketFactory;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.DFSClient.DFSOutputStream;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.*;

public class YuntiDistributedFileSystem extends DistributedFileSystem {
  private DistributedFileSystem onlineFS, archiveFS;
  private String fsCmdOption;
  private boolean onlyChooseOnline;
  private boolean onlyChooseArchive;

  public YuntiDistributedFileSystem() {
  }

  private boolean canConnectFS(URI uri, Configuration conf) {
    try {
      SocketFactory socketFactory = NetUtils.getDefaultSocketFactory(conf);
      Socket socket = socketFactory.createSocket();
      socket.setTcpNoDelay(true);
      InetSocketAddress namenode = NameNode.getAddress(uri.getAuthority());
      socket.connect(namenode, 20000);
    } catch (SocketTimeoutException toe) {
      return false;
    } catch (Exception ie) {
      return false;
    }
    return true;
  }
  private void initializeOnline(URI uri, Configuration conf) throws IOException {
    try {
      onlineFS = new DistributedFileSystem();
      onlineFS.initialize(uri, conf);
    } catch (Exception e) {
      System.out.println("Unable to Access Online Cluster.");
      throw new IOException(e);
    }
  }
  private void initializeArchive(URI uri, Configuration conf) throws IOException {
    try {
      URI archiveURI = URI.create(conf.get("fs.archive.name"));
      Configuration archiveConf = new Configuration(conf);
      archiveConf.set("fs.default.name", conf.get("fs.archive.name"));
      archiveFS = new DistributedFileSystem();
      archiveFS.initialize(archiveURI, archiveConf);
    } catch (Exception e) {
      System.out.println("Unable to Access Archive Cluster.");
      throw new IOException(e);
    }
  }
  private boolean enterWithOption() {
    fsCmdOption = YuntiFsShell.getCmdOption();
    if(  fsCmdOption != null &&
        (fsCmdOption.equals("-online") || fsCmdOption.equals("-archive") || fsCmdOption.equals("-all") ) ) {
      return true;
    }
    return false;
  }
  private void getOnlyChooseOnlineArchive() {
    fsCmdOption = YuntiFsShell.getCmdOption();
    onlyChooseArchive = fsCmdOption!=null && fsCmdOption.equals("-archive");
    onlyChooseOnline  = fsCmdOption!=null && fsCmdOption.equals("-online");
  }
  public void initialize(URI uri, Configuration conf) throws IOException {
    getOnlyChooseOnlineArchive();
    if(onlyChooseOnline) {
      initializeOnline(uri, conf);
      return;
    }
    else if(onlyChooseArchive) {
      initializeArchive(uri, conf);
      return;
    }
    
    int failTime = 0;
    try {
      onlineFS = new DistributedFileSystem();
      onlineFS.initialize(uri, conf);
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Unable to Access Online Cluster.");
      failTime++;
    }

    try {
      URI archiveURI = URI.create(conf.get("fs.archive.name"));
      Configuration archiveConf = new Configuration(conf);
      archiveConf.set("fs.default.name", conf.get("fs.archive.name"));
      if (this.canConnectFS(archiveURI, archiveConf)) {
        archiveFS = new DistributedFileSystem();
        archiveFS.initialize(archiveURI, archiveConf);
      }
    } catch (Exception e) {
      // System.out.println("Unable to Access Archive Cluster.");
      failTime++;
    }

    if (failTime == 2) {
      System.out.println("Unable to Access Both Online and Archive Clusters.");
      throw new IOException(
          "Unable to Access Both Online and Archive Clusters.");
    }
  }

  public URI getUri() {
    URI uri = onlineFS.getUri();
    if (uri == null)
      return archiveFS.getUri();
    else
      return uri;
  }

  /** Permit paths which explicitly specify the default port. */
  public void checkPath(Path path) {
    if (archiveFS == null) 
      onlineFS.checkPath(path); 
    try {
      onlineFS.checkPath(path);
    } catch (Exception e) {
      archiveFS.checkPath(path);
    }
  }

  /** Normalize paths that explicitly specify the default port. */
  public Path makeQualified(Path path) {
    return onlineFS.makeQualified(path);
  }

  public Path getWorkingDirectory() {
    return onlineFS.getWorkingDirectory();
  }

  public long getDefaultBlockSize() {
    return onlineFS.getDefaultBlockSize();
  }

  public short getDefaultReplication() {
    return onlineFS.getDefaultReplication();
  }

  public void setWorkingDirectory(Path dir) {
    onlineFS.setWorkingDirectory(dir);
  }

  public Path getHomeDirectory() {
    if (archiveFS == null)
      return onlineFS.getHomeDirectory();
    Path path;
    try {
      path = onlineFS.getHomeDirectory();
    } catch (NullPointerException e) {
      path = archiveFS.getHomeDirectory();
    }
    return path;
  }

  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
      long len) throws IOException {
    if (archiveFS == null)
      return onlineFS.getFileBlockLocations(file, start, len);
    BlockLocation[] locations = onlineFS
        .getFileBlockLocations(file, start, len);
    if (locations != null)
      return locations;
    else
      return archiveFS.getFileBlockLocations(file, start, len);
  }

  public void setVerifyChecksum(boolean verifyChecksum) {
    if(!onlyChooseArchive) {
    onlineFS.setVerifyChecksum(verifyChecksum);
    }
  }

  private FSDataInputStream openWithOption(Path f, int bufferSize)
      throws IOException {
    if (fsCmdOption.equals("-online")) {
      return onlineFS.open(f, bufferSize);
    } else if (fsCmdOption.equals("-archive")) {
      return archiveFS.open(f, bufferSize);
    } else if (fsCmdOption.equals("-all")) {
      return null;
    }
    return null;
  }

  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    if (enterWithOption()) {
      return openWithOption(f, bufferSize);
    }

    if (archiveFS == null)
      return onlineFS.open(f, bufferSize);
    FSDataInputStream stream;
    try {
      stream = onlineFS.open(f, bufferSize);
    } catch (Exception e) {
      return archiveFS.open(f, bufferSize);
    }
    if (stream != null)
      return stream;
    else
      return archiveFS.open(f, bufferSize);
  }

  /** This optional operation is not yet supported. */
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    throw new UnsupportedOperationException("HDFS does not support append yet");
  }

  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return onlineFS.create(f, permission, overwrite, bufferSize, replication,
        blockSize, progress);
  }

  public boolean setReplication(Path src, short replication) throws IOException {
    return onlineFS.setReplication(src, replication);
  }

  public boolean rename(Path src, Path dst) throws IOException {
    checkAllowOperation(src);
    return onlineFS.rename(src, dst);
  }

  @Deprecated
  public boolean delete(Path f) throws IOException {
    checkAllowOperation(f);
    return onlineFS.delete(f);
  }

  /**
   * requires a boolean check to delete a non empty directory recursively.
   */
  public boolean delete(Path f, boolean recursive) throws IOException {
    return onlineFS.delete(f, recursive);
  }

  /** {@inheritDoc} */
  public ContentSummary getContentSummary(Path f) throws IOException {
    return onlineFS.getContentSummary(f);
  }

  /**
   * Set a directory's quotas
   * 
   * @see org.apache.hadoop.hdfs.protocol.ClientProtocol#setQuota(String, long,
   *      long)
   */
  public void setQuota(Path src, long namespaceQuota, long diskspaceQuota)
      throws IOException {
    onlineFS.setQuota(src, namespaceQuota, diskspaceQuota);
  }

  public FileStatus[] globStatus(Path pathPattern, PathFilter filter)
      throws IOException {
    if (archiveFS == null)
      return onlineFS.globStatus(pathPattern, filter);
    FileStatus[] status;
    try {
      status = onlineFS.globStatus(pathPattern, filter);
    } catch (Exception e) {
      return archiveFS.globStatus(pathPattern, filter);
    }
    if (status != null && status.length > 0)
      return status;
    else
      return archiveFS.globStatus(pathPattern, filter);
  }

  private FileStatus[] globStatusWithOption(Path p) {
    if (fsCmdOption.equals("-online")) {
      try {
        return onlineFS.globStatus(p);
      } catch (Exception e1) {
        return null;
      }
    } else if (fsCmdOption.equals("-archive")) {
      try {
        return archiveFS.globStatus(p);
      } catch (Exception el) {
        return null;
      }
    } else if (fsCmdOption.equals("-all")) {
      return null;
    }
    return null;
  }

  public FileStatus[] globStatus(Path p) {
    if (enterWithOption()) {
      return globStatusWithOption(p);
    }

    if (archiveFS == null) {
      try {
        return onlineFS.globStatus(p);
      } catch (IOException e1) {
        return null;
      }
    }

    try {
      FileStatus[] status;
      try {
        status = onlineFS.globStatus(p);
      } catch (Exception e) {
        return archiveFS.globStatus(p);
      }
      if (status != null && status.length > 0) {
        return status;
      } else {
        return archiveFS.globStatus(p);
      }
    } catch (Exception e) {
      return null;
    }
  }

  private FileStatus[] listStatusWithOption(Path p) throws IOException {
    if (fsCmdOption.equals("-online")) {
      return onlineFS.listStatus(p);
    } else if (fsCmdOption.equals("-archive")) {
      return archiveFS.listStatus(p);
    } else if (fsCmdOption.equals("-all")) {
      return null;
    }
    return null;
  }

  public FileStatus[] listStatus(Path p) throws IOException {
    Path newPath = new Path(p.toUri().getPath());
    if (enterWithOption()) {
      return listStatusWithOption(newPath);
    }

    if (archiveFS == null)
      return onlineFS.listStatus(newPath);
    FileStatus[] status;
    try {
      status = onlineFS.listStatus(newPath);
    } catch (Exception e) {
      return archiveFS.listStatus(newPath);
    }
    if (status != null && status.length > 0)
      return status;
    else
      return archiveFS.listStatus(newPath);
  }

  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return onlineFS.mkdirs(f, permission);
  }

  public void close() throws IOException {
    int failTime = 0;
    if(!onlyChooseArchive)
    {
    try {
      onlineFS.close();
    } catch (Exception e) {
      failTime++;
    }
    }

    if(!onlyChooseOnline)
    try {
      archiveFS.close();
    } catch (Exception e) {
      failTime++;
    }

    if (failTime == 2) {
      System.out.println("Unable to Close both Online and Archive Clusters.");
      throw new IOException("Unable to Close both Online and Archive Clusters.");
    }
  }

  public String toString() {
    return onlineFS.toString();
  }

  public DFSClient getClient() {
    return onlineFS.getClient();
  }

  public DiskStatus getDiskStatus() throws IOException {
    return onlineFS.getDiskStatus();
  }

  public long getRawCapacity() throws IOException {
    return onlineFS.getRawCapacity();
  }

  public long getRawUsed() throws IOException {
    return onlineFS.getRawUsed();
  }

  public DatanodeInfo[] getDataNodeStats() throws IOException {
    return onlineFS.getDataNodeStats();
  }

  public boolean setSafeMode(FSConstants.SafeModeAction action)
      throws IOException {
    return onlineFS.setSafeMode(action);
  }

  public void refreshNodes() throws IOException {
    onlineFS.refreshNodes();
  }

  public void finalizeUpgrade() throws IOException {
    onlineFS.finalizeUpgrade();
  }

  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action)
      throws IOException {
    return onlineFS.distributedUpgradeProgress(action);
  }

  public void metaSave(String pathname) throws IOException {
    onlineFS.metaSave(pathname);
  }

  public boolean reportChecksumFailure(Path f, FSDataInputStream in,
      long inPos, FSDataInputStream sums, long sumsPos) {
    return onlineFS.reportChecksumFailure(f, in, inPos, sums, sumsPos);
  }

  private FileStatus getFileStatusWithOption(Path f) throws IOException {
    if (fsCmdOption.equals("-online")) {
      return onlineFS.getFileStatus(f);
    } else if (fsCmdOption.equals("-archive")) {
      return archiveFS.getFileStatus(f);
    } else if (fsCmdOption.equals("-all")) {
      return null;
    }
    return null;
  }

  public FileStatus getFileStatus(Path f) throws IOException {
    Path newPath = new Path(f.toUri().getPath());
    if (enterWithOption()) {
      return getFileStatusWithOption(newPath);
    }

    if (archiveFS == null)
      return onlineFS.getFileStatus(f);
    try {
      return onlineFS.getFileStatus(newPath);
    } catch (Exception e) {
      try {
        return archiveFS.getFileStatus(newPath);
      } catch (Exception e1) {
        throw new FileNotFoundException("File does not exist: " + newPath);
      }
    }
  }

  public MD5MD5CRC32FileChecksum getFileChecksum(Path f) throws IOException {
    //return onlineFS.getFileChecksum(f);
    try {
      return onlineFS.getFileChecksum(f);
    } catch (Exception e) {
      return archiveFS.getFileChecksum(f);
    }
  }

  private void checkAllowOperation(Path path) throws IOException {
    URI uri = path.toUri();
    if (uri.getScheme() == null) // fs is relative
      return;
    String thisScheme = onlineFS.getUri().getScheme();
    String thatScheme = uri.getScheme();
    String thisAuthority = onlineFS.getUri().getAuthority();
    String thatAuthority = uri.getAuthority();
    if (thisScheme.equalsIgnoreCase(thatScheme)) {// schemes match
      if (thisAuthority == thatAuthority || // & authorities match
          (thisAuthority != null && thisAuthority
              .equalsIgnoreCase(thatAuthority)))
        return;
      if (thatAuthority == null && // path's authority is null
          thisAuthority != null) { // fs has an authority
        URI defaultUri = getDefaultUri(getConf()); // & is the default fs
        if (thisScheme.equalsIgnoreCase(defaultUri.getScheme())
            && thisAuthority.equalsIgnoreCase(defaultUri.getAuthority()))
          return;
      }
    }
    throw new IOException("File:" + path.toUri().getRawPath()
        + " only exist in archive Cluster.");
  }

  public void setPermission(Path p, FsPermission permission) throws IOException {
    checkAllowOperation(p);
    onlineFS.setPermission(p, permission);
  }

  public void setOwner(Path p, String username, String groupname)
      throws IOException {
    checkAllowOperation(p);
    onlineFS.setOwner(p, username, groupname);
  }

  public void setTimes(Path p, long mtime, long atime) throws IOException {
    onlineFS.setTimes(p, mtime, atime);
  }
}