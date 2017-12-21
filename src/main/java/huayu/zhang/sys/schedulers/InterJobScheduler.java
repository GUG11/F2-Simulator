package huayu.zhang.sys.schedulers;

import huayu.zhang.sys.cluster.Cluster;
import huayu.zhang.sys.datastructures.BaseDag;
import huayu.zhang.sys.datastructures.Resources;
import huayu.zhang.sys.datastructures.StageDag;
import huayu.zhang.sys.sharepolicies.*;
import huayu.zhang.sys.simulator.Main.Globals;
import huayu.zhang.sys.simulator.Main.Globals.SharingPolicy;
import huayu.zhang.sys.simulator.Simulator;

import java.util.*;

// responsible for recomputing the resource share and update
// the resource counters for every running job
public class InterJobScheduler {

  public SharePolicy resSharePolicy;

  public InterJobScheduler(Cluster cluster) {

    switch (Globals.INTER_JOB_POLICY) {
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

  public void adjustShares(Cluster cluster) {
    List<Integer> unhappyDagsIds = new ArrayList<Integer>();

    final Map<Integer, Resources> unhappyDagsDistFromResShare = new HashMap<Integer, Resources>();
    for (BaseDag dag : Simulator.runningJobs) {
      if (!dag.rsrcQuota.distinct(dag.rsrcInUse)) {
        continue;
      }

      if (dag.rsrcInUse.greaterOrEqual(dag.rsrcQuota)) {
      } else {
        Resources farthestFromShare = Resources.subtract(dag.rsrcQuota,
            dag.rsrcInUse);
        unhappyDagsIds.add(dag.dagId);
        unhappyDagsDistFromResShare.put(dag.dagId, farthestFromShare);
      }
    }
    Collections.sort(unhappyDagsIds, new Comparator<Integer>() {
      public int compare(Integer arg0, Integer arg1) {
        Resources val0 = unhappyDagsDistFromResShare.get(arg0);
        Resources val1 = unhappyDagsDistFromResShare.get(arg1);
        return val0.compareTo(val1);
      }
    });

    /* System.out.println("Unhappy dags:");
    unhappyDagsIds.stream().forEach(x -> System.out.print(x + ",")); */

    // now try to allocate the available resources to dags in this order
    Resources availRes = Resources.clone(cluster.getClusterResAvail());
    // System.out.println("Resources for Unhappy dags:" + availRes);

    for (int dagId : unhappyDagsIds) {
      if (!availRes.greater(new Resources(0.0)))
        break;

      StageDag dag = Simulator.getDag(dagId);

      Resources rsrcReqTillShare = unhappyDagsDistFromResShare.get(dagId);
      // System.out.println("Dag:" + dagId + "requires " + rsrcReqTillShare);

      if (availRes.greaterOrEqual(rsrcReqTillShare)) {
        availRes.subtract(rsrcReqTillShare);
      } else {
        Resources toGive = Resources.piecewiseMin(availRes, rsrcReqTillShare);
        dag.rsrcQuota.copy(toGive);
        availRes.subtract(toGive);
      }
    }
  }

  // return the jobs IDs based on different policies
  // SJF: return ids should be based on SRTF
  // All other policies should be based on Fairness considerations
  public List<Integer> orderedListOfJobsBasedOnPolicy() {
    List<Integer> runningDagsIds = new ArrayList<Integer>();

    final Map<Integer, Resources> runnableDagsComparatorVal = new HashMap<Integer, Resources>();
    for (BaseDag dag : Simulator.runningJobs) {
      runningDagsIds.add(dag.dagId);
      Resources farthestFromShare = Resources.subtract(dag.rsrcQuota,
          dag.rsrcInUse);
      runnableDagsComparatorVal.put(dag.dagId, farthestFromShare);
    }
    Collections.sort(runningDagsIds, new Comparator<Integer>() {
      public int compare(Integer arg0, Integer arg1) {
        Resources val0 = runnableDagsComparatorVal.get(arg0);
        Resources val1 = runnableDagsComparatorVal.get(arg1);
        return val0.compareTo(val1);
      }
    });

    return runningDagsIds;
  }
}
