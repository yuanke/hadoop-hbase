package org.apache.hadoop.mapred;

import java.util.Comparator;

public class YTWeightComparator implements Comparator<JobInProgress> {
  private YTTaskType curType;
  private final YTPool curPool;
  public YTWeightComparator(YTPool pool, YTTaskType type) {
    this.curPool = pool;
    this.curType = type;
  }
  @Override
  public int compare(JobInProgress j1, JobInProgress j2) {
    double dif = curPool.weight(j2, curType) - curPool.weight(j1, curType);    
    return (int) Math.signum(dif);
  }

}
