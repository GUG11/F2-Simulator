package huayu.zhang.sys.schedulers;

import huayu.zhang.sys.cluster.Cluster;
import huayu.zhang.sys.datastructures.BaseDag;
import huayu.zhang.sys.datastructures.Resources;
import huayu.zhang.sys.datastructures.StageDag;
import huayu.zhang.sys.sharepolicies.*;
import huayu.zhang.sys.simulator.Main.Globals;
import huayu.zhang.sys.utils.Configuration.SharingPolicy;
import huayu.zhang.sys.simulator.Simulator;

import java.util.*;

// responsible for recomputing the resource share and update
// the resource counters for every running job
public class InterJobScheduler {
  public SharePolicy resSharePolicy;

  public InterJobScheduler(Cluster cluster, SharingPolicy sp) {
    switch (sp) {
    case Fair:
      resSharePolicy = new FairSharePolicy("Fair", cluster);
      break;
    case DRF:
      resSharePolicy = new DRFSharePolicy("DRF", cluster);
      break;
    default:
      System.err.println("Unknown sharing policy");
    }
  }

  public void schedule(Cluster cluster) {
    // compute how much share each DAG should get
    resSharePolicy.computeResShare(cluster);
  }
}
