package huayu.zhang.sys.schedpolicies;

import huayu.zhang.sys.cluster.Cluster;
import huayu.zhang.sys.datastructures.Resources;
import huayu.zhang.sys.datastructures.StageDag;

public abstract class SchedPolicy {

  protected Cluster cluster_;

  public SchedPolicy(Cluster cluster) {
    cluster_ = cluster;
  }

  public abstract void schedule(StageDag dag);

  public abstract double planSchedule(StageDag dag, Resources leftOverResources);
}
