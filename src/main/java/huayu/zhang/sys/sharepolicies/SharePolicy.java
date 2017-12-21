package huayu.zhang.sys.sharepolicies;

import huayu.zhang.sys.cluster.Cluster;

public abstract class SharePolicy {

  public String sharingPolicyName;

  public SharePolicy(String policyName) {
    sharingPolicyName = policyName;
  }

  // recompute the resource share allocated for every job
  public void computeResShare(Cluster cluster) {
  }

  public void packTasks(Cluster cluster) {}
}
