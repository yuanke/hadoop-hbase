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

import java.text.DecimalFormat;
import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.jar.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;

/** 
 * Utilities used in unit test.
 *  
 */
public class UtilsForTests {

  final static long KB = 1024L * 1;
  final static long MB = 1024L * KB;
  final static long GB = 1024L * MB;
  final static long TB = 1024L * GB;
  final static long PB = 1024L * TB;
  final static Object waitLock = new Object();

  static DecimalFormat dfm = new DecimalFormat("####.000");
  static DecimalFormat ifm = new DecimalFormat("###,###,###,###,###");

  public static String dfmt(double d) {
    return dfm.format(d);
  }

  public static String ifmt(double d) {
    return ifm.format(d);
  }

  public static String formatBytes(long numBytes) {
    StringBuffer buf = new StringBuffer();
    boolean bDetails = true;
    double num = numBytes;

    if (numBytes < KB) {
      buf.append(numBytes + " B");
      bDetails = false;
    } else if (numBytes < MB) {
      buf.append(dfmt(num / KB) + " KB");
    } else if (numBytes < GB) {
      buf.append(dfmt(num / MB) + " MB");
    } else if (numBytes < TB) {
      buf.append(dfmt(num / GB) + " GB");
    } else if (numBytes < PB) {
      buf.append(dfmt(num / TB) + " TB");
    } else {
      buf.append(dfmt(num / PB) + " PB");
    }
    if (bDetails) {
      buf.append(" (" + ifmt(numBytes) + " bytes)");
    }
    return buf.toString();
  }

  public static String formatBytes2(long numBytes) {
    StringBuffer buf = new StringBuffer();
    long u = 0;
    if (numBytes >= TB) {
      u = numBytes / TB;
      numBytes -= u * TB;
      buf.append(u + " TB ");
    }
    if (numBytes >= GB) {
      u = numBytes / GB;
      numBytes -= u * GB;
      buf.append(u + " GB ");
    }
    if (numBytes >= MB) {
      u = numBytes / MB;
      numBytes -= u * MB;
      buf.append(u + " MB ");
    }
    if (numBytes >= KB) {
      u = numBytes / KB;
      numBytes -= u * KB;
      buf.append(u + " KB ");
    }
    buf.append(u + " B"); //even if zero
    return buf.toString();
  }

  static final String regexpSpecials = "[]()?*+|.!^-\\~@";

  public static String regexpEscape(String plain) {
    StringBuffer buf = new StringBuffer();
    char[] ch = plain.toCharArray();
    int csup = ch.length;
    for (int c = 0; c < csup; c++) {
      if (regexpSpecials.indexOf(ch[c]) != -1) {
        buf.append("\\");
      }
      buf.append(ch[c]);
    }
    return buf.toString();
  }

  public static String safeGetCanonicalPath(File f) {
    try {
      String s = f.getCanonicalPath();
      return (s == null) ? f.toString() : s;
    } catch (IOException io) {
      return f.toString();
    }
  }

  static String slurp(File f) throws IOException {
    int len = (int) f.length();
    byte[] buf = new byte[len];
    FileInputStream in = new FileInputStream(f);
    String contents = null;
    try {
      in.read(buf, 0, len);
      contents = new String(buf, "UTF-8");
    } finally {
      in.close();
    }
    return contents;
  }

  static String slurpHadoop(Path p, FileSystem fs) throws IOException {
    int len = (int) fs.getLength(p);
    byte[] buf = new byte[len];
    InputStream in = fs.open(p);
    String contents = null;
    try {
      in.read(buf, 0, len);
      contents = new String(buf, "UTF-8");
    } finally {
      in.close();
    }
    return contents;
  }

  public static String rjustify(String s, int width) {
    if (s == null) s = "null";
    if (width > s.length()) {
      s = getSpace(width - s.length()) + s;
    }
    return s;
  }

  public static String ljustify(String s, int width) {
    if (s == null) s = "null";
    if (width > s.length()) {
      s = s + getSpace(width - s.length());
    }
    return s;
  }

  static char[] space;
  static {
    space = new char[300];
    Arrays.fill(space, '\u0020');
  }

  public static String getSpace(int len) {
    if (len > space.length) {
      space = new char[Math.max(len, 2 * space.length)];
      Arrays.fill(space, '\u0020');
    }
    return new String(space, 0, len);
  }

  /**
   * A utility that waits for specified amount of time
   */
  public static void waitFor(long duration) {
    try {
      synchronized (waitLock) {
        waitLock.wait(duration);
      }
    } catch (InterruptedException ie) {}
  }
  
  static void writeFile(NameNode namenode, Configuration conf, Path name, 
      short replication) throws IOException {
    FileSystem fileSys = FileSystem.get(conf);
    SequenceFile.Writer writer = 
      SequenceFile.createWriter(fileSys, conf, name, 
                                BytesWritable.class, BytesWritable.class,
                                CompressionType.NONE);
    writer.append(new BytesWritable(), new BytesWritable());
    writer.close();
    fileSys.setReplication(name, replication);
    DFSTestUtil.waitReplication(fileSys, name, replication);
  }
  
  // Start a job and return its RunningJob object
  static RunningJob runJob(JobConf conf, Path inDir, Path outDir)
                    throws IOException {
    return runJob(conf, inDir, outDir, conf.getNumMapTasks(), conf.getNumReduceTasks());
  }

  // Start a job and return its RunningJob object
  static RunningJob runJob(JobConf conf, Path inDir, Path outDir, int numMaps, 
                           int numReds) throws IOException {

    FileSystem fs = FileSystem.get(conf);
    fs.delete(outDir, true);
    if (!fs.exists(inDir)) {
      fs.mkdirs(inDir);
    }
    String input = "The quick brown fox\n" + "has many silly\n"
        + "red fox sox\n";
    for (int i = 0; i < numMaps; ++i) {
      DataOutputStream file = fs.create(new Path(inDir, "part-" + i));
      file.writeBytes(input);
      file.close();
    }    

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setNumMapTasks(numMaps);
    conf.setNumReduceTasks(numReds);

    JobClient jobClient = new JobClient(conf);
    RunningJob job = jobClient.submitJob(conf);

    return job;
  }
}
