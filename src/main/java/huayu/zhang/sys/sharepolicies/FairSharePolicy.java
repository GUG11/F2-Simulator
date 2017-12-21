package huayu.zhang.sys.sharepolicies;

import huayu.zhang.sys.cluster.Cluster;
import huayu.zhang.sys.datastructures.BaseDag;
import huayu.zhang.sys.datastructures.Resources;
import huayu.zhang.sys.simulator.Simulator;

import java.util.logging.Logger;

public class FairSharePolicy extends SharePolicy {
  private static Logger LOG = Logger.getLogger(FairSharePolicy.class.getName());
  Resources clusterTotCapacity = null;

  public FairSharePolicy(String policyName, Cluster cluster) {
    super(policyName);
    clusterTotCapacity = cluster.getClusterMaxResAlloc();
  }

  // FairShare = 1 / N across all dimensions
  // N - total number of running jobs
  @Override
  public void computeResShare(Cluster cluster) {
    int numJobsRunning = Simulator.runningJobs.size();
    if (numJobsRunning == 0) {
      return;
    }

    Resources quotaRsrcShare = Resources.divide(clusterTotCapacity,
        numJobsRunning);

    // update the resourceShareAllocated for every running job
    for (BaseDag job : Simulator.runningJobs) {
      job.rsrcQuota = quotaRsrcShare;
      LOG.fine("Allocated to job:" + job.dagId + " share:"
        + job.rsrcQuota);
    }
  }
}
