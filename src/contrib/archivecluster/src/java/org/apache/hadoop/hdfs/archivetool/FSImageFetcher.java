package org.apache.hadoop.hdfs.archivetool;

import java.io.*;
import java.net.*;
import java.util.Iterator;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;

/**
 * @author weichao
 * This class provides fetching a specified file from one FileSystem
 */
class FSImageFetcher implements FSConstants {

  static void getFileClient(String fsName, String id, String dir)
    throws IOException {
    byte[] buf = new byte[BUFFER_SIZE];
    StringBuffer str = new StringBuffer("http://"+fsName+"/getimage?");
    str.append(id);
    
    System.out.println("Getting:"+str);
    
    URL url = new URL(str.toString());
    URLConnection connection = url.openConnection();
    InputStream stream = connection.getInputStream();
    FileOutputStream output = null;

    File localPath = new File(dir);
    
    try {
      if (localPath != null) {
        output = new FileOutputStream(localPath);
      }
      int num = 1;
      while (num > 0) {
        num = stream.read(buf);
        if (num > 0 && localPath != null) {
            output.write(buf, 0, num);
        }
      }
    } finally {
      stream.close();
      if (output != null) {
          if (output != null) {
            output.close();
          }
      }
    }
  }
}
