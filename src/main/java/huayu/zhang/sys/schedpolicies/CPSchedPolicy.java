package huayu.zhang.sys.schedpolicies;

import huayu.zhang.sys.cluster.Cluster;
import huayu.zhang.sys.datastructures.Resources;
import huayu.zhang.sys.datastructures.StageDag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import java.util.logging.Logger;
/**
 * CP scheduling class
 * */
public class CPSchedPolicy extends SchedPolicy {

  private static Logger LOG = Logger.getLogger(CPSchedPolicy.class.getName());
  public CPSchedPolicy(Cluster cluster) {
    super(cluster);
  }

  @Override
  public void schedule(final StageDag dag, double currentTime) {

    // no tasks to be scheduled -> skip
    LOG.fine("Dag: " + dag.getDagId() + " runnable tasks:" + dag.runnableTasks.size());
    if (dag.runnableTasks.isEmpty())
      return;

    ArrayList<Integer> rtCopy = new ArrayList<Integer>(dag.runnableTasks);

    // among the runnable tasks:
    // pick one which has the largest CP
    // as the next task to schedule
    Collections.sort(rtCopy, new Comparator<Integer>() {
      @Override
      public int compare(Integer arg0, Integer arg1) {
        Double val0 = dag.getCPLength(arg0);
        Double val1 = dag.getCPLength(arg1);
        return Double.compare(val0, val1);
      }
    });
    Iterator<Integer> iter = rtCopy.iterator();
    // dag.printCPLength();
    while (iter.hasNext()) {
      int taskId = iter.next();
      LOG.finest("Task " + taskId + " to be scheduled");

      // discard tasks whose resource requirements are larger than total share
      boolean fit = dag.currResShareAvailable().greaterOrEqual(
          dag.rsrcDemands(taskId));
      if (!fit) {
        LOG.finest("Task " + taskId + " does not fit quota " + dag.getDagId() + ". task resource demands: " + dag.rsrcDemands(taskId) + " dag resource available: " + dag.currResShareAvailable());
        continue;
      }

      // try to assign the next task on a machine
      int preferedMachine = dag.getAssignedMachine(taskId);
      boolean assigned = false;
      if (preferedMachine == -1) {
        assigned = cluster_.assignTask(dag, taskId,
            dag.duration(taskId), dag.rsrcDemands(taskId), currentTime);
      } else {
        assigned = cluster_.assignTask(preferedMachine, dag, taskId,
            dag.duration(taskId), dag.rsrcDemands(taskId), currentTime);
      }

      if (assigned) {
        // remove the task from runnable and put it in running
        dag.runningTasks.add(taskId);
        dag.launchedTasksNow.add(taskId);
        iter.remove();
        dag.runnableTasks.remove(taskId);
        String stage = dag.findStageByTaskID(taskId);
        if (!dag.isRunningStage(stage)) {
          dag.moveRunnableToRunning(stage);
        }
      }
    }

    dag.printLaunchedTasks();
    // clear the list of tasks launched as of now
    dag.launchedTasksNow.clear();
  }

}
