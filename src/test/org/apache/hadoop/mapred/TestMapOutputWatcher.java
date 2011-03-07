package org.apache.hadoop.mapred;

import java.io.IOException;
import junit.framework.TestCase;

public class TestMapOutputWatcher extends TestCase {
  
  private final String localDir0 = "/disk0/";
  private final String localDir1 = "/disk1/";
  private final String jobId0 = "job_000_0";
  private final String jobId1 = "job_000_1";
  private final long DEFAULT_MAX_MAPOUTPUT_SIZE_SKIP = 64 * 1024;
  private final long largeOutput = DEFAULT_MAX_MAPOUTPUT_SIZE_SKIP + 1;
  private final long smallOutput = DEFAULT_MAX_MAPOUTPUT_SIZE_SKIP - 1;
  
  private void setup(JobConf conf, int workers, int numDisks, float maxShufflePerJob) {
    conf.setInt("tasktracker.http.threads", workers);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numDisks; i++) {
      sb.append("/disk" + i + ',');
    }
    conf.set("mapred.local.dir", sb.toString());
    
    conf.set("mapred.job.shuffle.max.percent", Float.toString(maxShufflePerJob));
    conf.setLong("mapred.mapoutput.max.size.skip", DEFAULT_MAX_MAPOUTPUT_SIZE_SKIP);
  }
  
  public void testMaxShufflePerDisk() throws IOException {
    JobConf conf = new JobConf();
    
    // case: machine with six disks, 32 work threads per disk allowed
    setup(conf, 100, 6, 0.5f);
    MapOutputWatcher watcher0 = new MapOutputWatcher(conf);
    for (int i = 0; i < 100 / 6 * 2; i++) {
      assertTrue("Shuffle refused", watcher0.canShuffleAndInr(localDir0, jobId0, largeOutput));
    }
    
    assertFalse("Shuffle accepted", watcher0.canShuffleAndInr(localDir0, jobId0, largeOutput));
    watcher0.shuffleDecr(localDir0, jobId0);
    watcher0.shuffleDecr(localDir0, jobId0);
    assertTrue("Shuffle refused", watcher0.canShuffleAndInr(localDir0, jobId0, largeOutput));
    assertFalse("Shuffle accepted", watcher0.canShuffleAndInr(localDir0, jobId0, largeOutput));
    watcher0.shuffleDecr(localDir0, jobId0);
    assertTrue("Shuffle refused", watcher0.canShuffleAndInr(localDir1, jobId0, largeOutput));
    watcher0.shuffleDecr(localDir1, jobId0);

    for (int i = 0; i < 100 / 6 * 2; i++) {
      watcher0.shuffleDecr(localDir0, jobId0);
    }
    
    // case: machine with twelve disks, 16 work threads per disk allowed
    setup(conf, 100, 12, 0.5f);
    MapOutputWatcher watcher1 = new MapOutputWatcher(conf);
    for (int i = 0; i < 100 / 12 * 2; i++) {
      assertTrue("Shuffle refused", watcher1.canShuffleAndInr(localDir0, jobId0, largeOutput));
    }
    
    assertFalse("Shuffle accepted", watcher1.canShuffleAndInr(localDir0, jobId0, largeOutput));
    watcher1.shuffleDecr(localDir0, jobId0);
    watcher1.shuffleDecr(localDir0, jobId0);
    assertTrue("Shuffle refused", watcher1.canShuffleAndInr(localDir0, jobId0, largeOutput));
    assertFalse("Shuffle accepted", watcher1.canShuffleAndInr(localDir0, jobId0, largeOutput));
    watcher1.shuffleDecr(localDir0, jobId0);
    assertTrue("Shuffle refused", watcher1.canShuffleAndInr(localDir1, jobId0, largeOutput));
    watcher1.shuffleDecr(localDir1, jobId0);

    for (int i = 0; i < 100 / 12 * 2; i++) {
      watcher1.shuffleDecr(localDir0, jobId0);
    }
  }
  
  public void testMaxShufflePerJob() throws IOException {
    JobConf conf = new JobConf();
    
    // case: one job can only occupied zero thread concurrently
    setup(conf, 40, 12, 0.0f);
    MapOutputWatcher watcher0 = new MapOutputWatcher(conf);
    assertFalse("Shuffle accepted", watcher0.canShuffleAndInr(localDir0, jobId0, largeOutput));
    watcher0.shuffleDecr(localDir0, jobId0);
    
    // case: one job can only occupied one thread concurrently
    setup(conf, 40, 12, 0.03f); // (int) (40 * 0.03 + 0.5) = 1 
    MapOutputWatcher watcher1 = new MapOutputWatcher(conf);
    assertTrue("Shuffle refused", watcher1.canShuffleAndInr(localDir0, jobId0, largeOutput));
    assertFalse("Shuffle accepted", watcher1.canShuffleAndInr(localDir0, jobId0, largeOutput));
    watcher1.shuffleDecr(localDir0, jobId0);
    assertTrue("Shuffle refused", watcher1.canShuffleAndInr(localDir1, jobId1, largeOutput));
    watcher1.shuffleDecr(localDir1, jobId1);
    watcher1.shuffleDecr(localDir0, jobId0);
    assertTrue("Shuffle refused", watcher1.canShuffleAndInr(localDir0, jobId0, largeOutput));
    watcher1.shuffleDecr(localDir0, jobId0);
    
    // case: one job can occupied five thread concurrently
    setup(conf, 40, 12, 0.12f); // (int) (40 * 0.12 + 0.5) = 5
    MapOutputWatcher watcher2 = new MapOutputWatcher(conf);
    assertTrue("Shuffle refused", watcher2.canShuffleAndInr(localDir0, jobId0, largeOutput));
    assertTrue("Shuffle refused", watcher2.canShuffleAndInr(localDir0, jobId0, largeOutput));
    assertTrue("Shuffle refused", watcher2.canShuffleAndInr(localDir0, jobId0, largeOutput));
    assertTrue("Shuffle refused", watcher2.canShuffleAndInr(localDir0, jobId0, largeOutput));
    assertTrue("Shuffle refused", watcher2.canShuffleAndInr(localDir0, jobId0, largeOutput));
    assertFalse("Shuffle accepted", watcher2.canShuffleAndInr(localDir0, jobId0, largeOutput));
    watcher2.shuffleDecr(localDir0, jobId0);
    assertTrue("Shuffle refused", watcher1.canShuffleAndInr(localDir0, jobId1, largeOutput));
    watcher1.shuffleDecr(localDir0, jobId1);
    watcher2.shuffleDecr(localDir0, jobId0);
    assertTrue("Shuffle refused", watcher2.canShuffleAndInr(localDir0, jobId0, largeOutput));
    watcher2.shuffleDecr(localDir0, jobId0);
    watcher2.shuffleDecr(localDir0, jobId0);
    watcher2.shuffleDecr(localDir0, jobId0);
    watcher2.shuffleDecr(localDir0, jobId0);
    watcher2.shuffleDecr(localDir0, jobId0);
  }
  
  public void testShuffleSamllOutput() throws IOException {
    JobConf conf = new JobConf();

    // case: machine with twelve disks, 16 work threads per disk allowed
    setup(conf, 100, 12, 0.5f);
    MapOutputWatcher watcher1 = new MapOutputWatcher(conf);
    for (int i = 0; i < 100 / 12 * 2; i++) {
      assertTrue("Shuffle refused", watcher1.canShuffleAndInr(localDir0, jobId0, largeOutput));
    }
    
    assertFalse("Shuffle accepted", watcher1.canShuffleAndInr(localDir0, jobId0, largeOutput));
    watcher1.shuffleDecr(localDir0, jobId0);
    assertTrue("Shuffle refused", watcher1.canShuffleAndInr(localDir0, jobId0, smallOutput));
    watcher1.shuffleDecr(localDir0, jobId0);


    for (int i = 0; i < 100 / 12 * 2; i++) {
      watcher1.shuffleDecr(localDir0, jobId0);
    }
  }

}
