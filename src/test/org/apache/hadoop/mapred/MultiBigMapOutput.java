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

import java.io.IOException;
import java.util.Date;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.SortValidator.RecordStatsChecker.NonSplitableSequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MultiBigMapOutput extends Configured implements Tool {
  public static final Log LOG =
    LogFactory.getLog(MultiBigMapOutput.class.getName());
  private static Random random = new Random();
  
  private static void randomizeBytes(byte[] data, int offset, int length) {
    for(int i=offset + length - 1; i >= offset; --i) {
      data[i] = (byte) random.nextInt(256);
    }
  }

  private static void createBigMapInputFile(Configuration conf, FileSystem fs, 
      Path dir, long fileSizeInMB, int fileNum) 
throws IOException {
    // Check if the input path exists and is non-empty
    if (fs.exists(dir)) {
      FileStatus[] list = fs.listStatus(dir);
      if (list != null && list.length > 0) {
        throw new IOException("Input path: " + dir + " already exists... ");
      }
    }
    
    for (int i = 0; i < fileNum; i++) {
      String fileName = "part-" + i;
      createBigMapInputFile(conf, fs, new Path(dir, fileName), fileSizeInMB);
    }
    
  }
  
  private static void createBigMapInputFile(Configuration conf, FileSystem fs, 
                                            Path file, long fileSizeInMB) 
  throws IOException {
    SequenceFile.Writer writer = 
      SequenceFile.createWriter(fs, conf, file, 
                                BytesWritable.class, BytesWritable.class,
                                CompressionType.NONE);
    long numBytesToWrite = fileSizeInMB * 1024 * 1024;
    int minKeySize = conf.getInt("test.bmo.min_key", 10);;
    int keySizeRange = 
      conf.getInt("test.bmo.max_key", 1000) - minKeySize;
    int minValueSize = conf.getInt("test.bmo.min_value", 0);
    int valueSizeRange = 
      conf.getInt("test.bmo.max_value", 20000) - minValueSize;
    BytesWritable randomKey = new BytesWritable();
    BytesWritable randomValue = new BytesWritable();

    LOG.info("Writing " + numBytesToWrite + " bytes to " + file + " with " +
             "minKeySize: " + minKeySize + " keySizeRange: " + keySizeRange +
             " minValueSize: " + minValueSize + " valueSizeRange: " + valueSizeRange);
    long start = System.currentTimeMillis();
    while (numBytesToWrite > 0) {
      int keyLength = minKeySize + 
        (keySizeRange != 0 ? random.nextInt(keySizeRange) : 0);
      randomKey.setSize(keyLength);
      randomizeBytes(randomKey.getBytes(), 0, randomKey.getLength());
      int valueLength = minValueSize +
        (valueSizeRange != 0 ? random.nextInt(valueSizeRange) : 0);
      randomValue.setSize(valueLength);
      randomizeBytes(randomValue.getBytes(), 0, randomValue.getLength());
      writer.append(randomKey, randomValue);
      numBytesToWrite -= keyLength + valueLength;
    }
    writer.close();
    long end = System.currentTimeMillis();

    LOG.info("Created " + file + " of size: " + fileSizeInMB + "MB in " + 
             (end-start)/1000 + "secs");
  }
  
  private static void usage() {
    System.err.println("MultiBigMapOutput -input <input-dir> -output <output-dir> " +
                       "[-create <filesize in MB> <fileNum>] [-r <numReduce>]");
    ToolRunner.printGenericCommandUsage(System.err);
    System.exit(1);
  }
  public int run(String[] args) throws Exception {    
    if (args.length <= 4) { //input-dir should contain a huge file ( > 2GB)
      usage();
    } 
    Path bigMapInput = null;
    Path outputPath = null;
    boolean createInput = false;
    long fileSizeInMB = 3 * 1024;         // default of 3GB (>2GB)
    int fileNum = 1; // default one input file
    int numReduce = 1;
    for(int i=0; i < args.length; ++i) {
      if ("-input".equals(args[i])){
        bigMapInput = new Path(args[++i]);
      } else if ("-output".equals(args[i])){
        outputPath = new Path(args[++i]);
      } else if ("-r".equals(args[i])){
        numReduce = Integer.parseInt(args[++i]);
      } else if ("-create".equals(args[i])) {
        createInput = true;
        fileSizeInMB = Long.parseLong(args[++i]);
        fileNum = Integer.parseInt(args[++i]);
      } else {
        usage();
      }
    }
    
    FileSystem fs = FileSystem.get(getConf());
    JobConf jobConf = new JobConf(getConf(), MultiBigMapOutput.class);

    jobConf.setJobName("MultiBigMapOutput");
    jobConf.setInputFormat(NonSplitableSequenceFileInputFormat.class);
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(jobConf, bigMapInput);
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }
    FileOutputFormat.setOutputPath(jobConf, outputPath);
    jobConf.setMapperClass(IdentityMapper.class);
    jobConf.setReducerClass(IdentityReducer.class);
    jobConf.setNumReduceTasks(numReduce);
    jobConf.setOutputKeyClass(BytesWritable.class);
    jobConf.setOutputValueClass(BytesWritable.class);
    
    if (createInput) {
      createBigMapInputFile(jobConf, fs, bigMapInput, fileSizeInMB, fileNum);
    }
    
    Date startTime = new Date();
    System.out.println("Job started: " + startTime);
    JobClient.runJob(jobConf);
    Date end_time = new Date();
    System.out.println("Job ended: " + end_time);
    
    return 0;
  }

  public static void main(String argv[]) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MultiBigMapOutput(), argv);
    System.exit(res);
  }

}
