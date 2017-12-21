package huayu.zhang.sys.schedpolicies;

import huayu.zhang.sys.cluster.Cluster;
import huayu.zhang.sys.datastructures.Resources;
import huayu.zhang.sys.datastructures.StageDag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public class RandomSchedPolicy extends SchedPolicy {

  public RandomSchedPolicy(Cluster cluster) {
    super(cluster);
  }

  @Override
  public void schedule(StageDag dag) {

    if (dag.runnableTasks.isEmpty())
      return;

    ArrayList<Integer> rtCopy = new ArrayList<Integer>(dag.runnableTasks);

    // among the runnable tasks:
    // pick one which has the largest CP
    // as the next task to schedule
    Collections.shuffle(rtCopy);

    Iterator<Integer> iter = rtCopy.iterator();
    while (iter.hasNext()) {
      int taskId = iter.next();

      // discard tasks whose resource requirements are larger than total share
      boolean fit = dag.currResShareAvailable().greaterOrEqual(
          dag.rsrcDemands(taskId));
      if (!fit)
        continue;

      // try to assign the next task on a machine
      boolean assigned = cluster_.assignTask(dag, taskId,
          dag.duration(taskId), dag.rsrcDemands(taskId));

      if (assigned) {
        // remove the task from runnable and put it in running
        dag.runningTasks.add(taskId);
        iter.remove();
        dag.runnableTasks.remove(taskId);
      }
    }
  }

  @Override
  public double planSchedule(StageDag dag, Resources leftOverResources) {
    return -1;
  }
}
