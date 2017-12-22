package huayu.zhang.sys.schedulers;

import huayu.zhang.sys.cluster.Cluster;
import huayu.zhang.sys.datastructures.Resources;
import huayu.zhang.sys.datastructures.StageDag;
import huayu.zhang.sys.schedpolicies.*;
import huayu.zhang.sys.utils.Configuration.SchedulingPolicy;

// responsible for scheduling tasks inside a Job
// it can adopt various strategies such as Random, Critical Path
// BFS, Tetris, Graphene, etc.

// return unnecessary resources to the resource pool
public class IntraJobScheduler {

  public SchedPolicy resSchedPolicy;

  public IntraJobScheduler(Cluster cluster, SchedulingPolicy schedPolicy) {

    switch (schedPolicy) {
    case CP:
      resSchedPolicy = new CPSchedPolicy(cluster);
      break;
    default:
      System.err.println("Unknown sharing policy");
    }
  }

  public void schedule(StageDag dag) {
    // while tasks can be assigned in my resource
    // share quanta, on any machine, keep assigning
    // otherwise return
    resSchedPolicy.schedule(dag);
  }

  public double planSchedule(StageDag dag, Resources leftOverResources) {
    return resSchedPolicy.planSchedule(dag, leftOverResources);
  }
}
