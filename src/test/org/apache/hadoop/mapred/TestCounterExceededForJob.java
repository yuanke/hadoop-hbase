package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class TestCounterExceededForJob extends ClusterMapReduceTestCase{

  private static final JobConf conf = new JobConf();
  /** limit on the size of the name of the group **/
  private static final int GROUP_NAME_LIMIT = 
    conf.getInt("mapred.counter.groupname.limit", 128);
  /** limit on the size of the counter name **/ 
  private static final int COUNTER_NAME_LIMIT = 
    conf.getInt("mapred.countername.limit", 64);

  /** the max number of counters **/
  static final int MAX_COUNTER_LIMIT = conf.getInt("mapred.job.counters.limit", 120);
  
  /**
   * Create the JobConf with our mapper and reducer, etc.
   * @return JobConf
   */
  private JobConf createJob() {
    JobConf conf = createJobConf();
    conf.setJobName("counters");
    conf.setMapOutputKeyClass(LongWritable.class);
    conf.setMapOutputValueClass(Text.class);
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(CounterMapper.class);
    conf.setReducerClass(IdentityReducer.class);

    FileInputFormat.setInputPaths(conf, getInputDir());
    FileOutputFormat.setOutputPath(conf, getOutputDir());
    
    return conf;
  }
  
  /**
   * This mapper class is based on IdentityMaper. However, we define
   * 10 counters for each value as group name.
   */
  static class CounterMapper<K, V> extends IdentityMapper<K, V> {
    String prefix = "hello";
    public void configure(JobConf job) {
      prefix = job.get("counter.name.prefix", "hello");
    }
    public void map(K key, V value,
        OutputCollector<K, V> output, Reporter reporter)
    throws IOException {
      output.collect(key, value);

      for (int i = 0; i < 10; i++)
        reporter.incrCounter(value.toString(), prefix + i, 1);
    }
  }
  
  /**
   * If the counter number of a task exceeds limit, this task will fail.
   * Then the job will fail.
   * @throws Exception
   */ 
  public void testTaskCounterNumExceedTask() throws Exception {
    OutputStream os = getFileSystem().create(new Path(getInputDir(), "text.txt"));
    Writer wr = new OutputStreamWriter(os);
    for (int i = 0; i < (MAX_COUNTER_LIMIT + 9)/ 10; i++)
      wr.write("hello" + i + "\n");
    wr.close();
    
    JobConf conf = createJob();
    conf.setNumMapTasks(1);

    boolean success = false;
    try {
      JobClient.runJob(conf);   
      success = true;
    } catch (IOException e) {
    }
    assertEquals(false, success);
  }
  
  /**
   * If a counter's name string length exceeds the limit, the task will fail.
   * Then the job will fail.
   * @throws Exception
   */
  public void testTaskCounterNameLengthExceedJob() throws Exception {
    OutputStream os = getFileSystem().create(new Path(getInputDir(), "text.txt"));
    Writer wr = new OutputStreamWriter(os);
    wr.write("hello\n");
    wr.close();

    JobConf conf = createJob();
    conf.setNumMapTasks(1);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < COUNTER_NAME_LIMIT; i++)
      sb.append('A');
    conf.set("counter.name.prefix", sb.toString());// 64 characters

    boolean success = false;
    try {
      JobClient.runJob(conf);   
      success = true;
    } catch (IOException e) {
    }
    assertEquals(false, success);
  }
  
  /**
   * If a counter group's name string length exceeds the limit, the task will fail.
   * Then the job will fail.
   * @throws Exception
   */
  public void testTaskCounterGroupNameLengthExceedJob() throws Exception {
    OutputStream os = getFileSystem().create(new Path(getInputDir(), "text.txt"));
    Writer wr = new OutputStreamWriter(os);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < GROUP_NAME_LIMIT + 1; i++)
      sb.append('A');
    wr.write(sb.toString());
    wr.close();
    
    JobConf conf = createJob();
    conf.setNumMapTasks(1);

    boolean success = false;
    try {
      JobClient.runJob(conf);   
      success = true;
    } catch (IOException e) {
    }
    assertEquals(false, success);
  }
  
  /**
   * If we don't call getCounters(), JobTracker will not combine tasks' counters together.
   * Then the job will finish successfully.
   * @throws Exception
   */
  public void testTaskCounterNumExceedJobWithoutCombine() throws Exception {
    OutputStream os = getFileSystem().create(new Path(getInputDir(), "text.txt"));
    Writer wr = new OutputStreamWriter(os);
    for (int i = 0; i < (MAX_COUNTER_LIMIT + 9) / 10; i++)
      wr.write("hello" + i + "\n");
    wr.close();
    
    JobConf conf = createJob();
    conf.setNumMapTasks(2);

    boolean success = false;
    try {
      JobClient.runJob(conf);   
      success = true;
    } catch (IOException e) {
    }
    assertEquals(true, success);
  }
  
  /**
   * If we call getCounters(), JobTracker will combine tasks' counters together.
   * Then total counter number per job will exceed limit and the job will fail.
   * @throws Exception
   */
  public void testTaskCounterNumExceedJobWithCombine() throws Exception {
    OutputStream os = getFileSystem().create(new Path(getInputDir(), "text.txt"));
    Writer wr = new OutputStreamWriter(os);
    for (int i = 0; i < (MAX_COUNTER_LIMIT + 9) / 10; i++)
      wr.write("hello" + i + "\n");
    wr.close();
    
    JobConf conf = createJob();
    conf.setNumMapTasks(2);

    RunningJob runningJob = new JobClient(conf).submitJob(conf);
    while (!runningJob.isComplete()) {
      runningJob.getCounters();
    }
      
    assertEquals(false, runningJob.isSuccessful());
  }
  
  private void groupnameExceed() {
    Counters counters = new Counters();

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < GROUP_NAME_LIMIT + 1; i++)
      sb.append('A');

    String err = "";
    try {
      counters.incrCounter(sb.toString(), "hello", 1);
    } catch (Exception e) {
      err = e.getMessage();
    }
    assertEquals("Exceed the max string size of a group name: " +
        "GroupName = " + sb.toString() + " Limit: " + GROUP_NAME_LIMIT, err);
  }
  
  private void counternameExceed() {
    Counters counters = new Counters();

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < COUNTER_NAME_LIMIT + 1; i++)
      sb.append('A');

    String err = "";
    try {
      counters.incrCounter("hello", sb.toString(), 1);
    } catch (Exception e) {
      err = e.getMessage();
    }
    assertEquals("Exceed the max string size of a counter name: " 
        + "CounterName = " + sb.toString() + " Limit: " + COUNTER_NAME_LIMIT, err);
  }
  
  private void counterNumExceed() {
    Counters counters = new Counters();

    for (int i = 0; i < MAX_COUNTER_LIMIT + 1; i++) { 
        counters.incrCounter("hello", "yunti" + i, 1); 
    }

    String err = "";
    try {
      counters.incrCounter("hello", "yunti", 1);
    } catch (Exception e) {
      err = e.getMessage();
    }

    assertEquals(MAX_COUNTER_LIMIT + 1, counters.size());
    assertEquals("Exceed the max number of counters per job: " 
        + "Num = " + (MAX_COUNTER_LIMIT + 1) + " Limit: " + MAX_COUNTER_LIMIT, err);
  }
  
  private void counterNumExceedWithCombine() {
    Counters counters1 = new Counters();
    Counters counters2 = new Counters();

    for (int i = 0; i < (MAX_COUNTER_LIMIT+1) / 2 + 1; i++) { 
      counters1.incrCounter("hello1", "yunti" + i, 1);
      counters2.incrCounter("hello2", "yunti" + i, 1);
    }
    
    assertEquals((MAX_COUNTER_LIMIT+1) / 2 + 1, counters1.size());
    assertEquals((MAX_COUNTER_LIMIT+1) / 2 + 1, counters2.size());

    String err = "";
    try {
      counters1.incrAllCounters(counters2);
    } catch (Exception e) {
      err = e.getMessage();
    }

    assertEquals(MAX_COUNTER_LIMIT + 1, counters1.size());
    assertEquals("Exceed the max number of counters per job: " 
        + "Num = " + (MAX_COUNTER_LIMIT + 1) + " Limit: " + MAX_COUNTER_LIMIT, err);
  }
  
  /**
   * Test four exception cases for the Counter class.
   * @throws Exception
   */
  public void testCountersException() throws Exception {
    groupnameExceed();
    counternameExceed();
    counterNumExceed();
    counterNumExceedWithCombine();
  }
}
