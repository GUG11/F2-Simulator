package huayu.zhang.sys.cluster;

import huayu.zhang.sys.F2.SpillEvent;
import huayu.zhang.sys.datastructures.Resources;
import huayu.zhang.sys.simulator.Simulator;
import huayu.zhang.sys.datastructures.StageDag;

import java.util.*;
import java.util.logging.Logger;

/**
 * describes the cluster characteristics both per cluster and per machine
* properties
 */
public class Cluster {

  boolean execMode;
  // public Map<Integer, Machine> machines;
  private List<Machine> machines;

  private static Logger LOG = Logger.getLogger(Cluster.class.getName());

  public Cluster(boolean state) {
    execMode = state;
    machines = new ArrayList<Machine>();
  }

  public void addMachine(Machine machine) {
    machines.add(machine);
  }

  public boolean assignTask(int machineId, StageDag dag, int taskId,
      double taskDuration, Resources taskResources, double currentTime) {
    LOG.fine("assign task: "+taskId+" from dag:"+ dag.getDagId() +" on machine:"+machineId);
    Machine machine = machines.get(machineId);
    assert (machine != null);
    boolean fit = machine.getTotalResAvail().greaterOrEqual(taskResources);
    if (!fit) {
      LOG.warning("ERROR: dag " + dag.getDagId() + ", stage:" + dag.findStageByTaskID(taskId) +  ", task: " + taskId +  " should fit");
      return false;
    }
    machine.assignTask(dag, taskId, taskDuration, taskResources, currentTime);
    return true;
  }

  // checks for fitting in resShare should already be done
  public boolean assignTask(StageDag dag, int taskId, double taskDuration,
      Resources taskResources, double currentTime) {

    // find the first machine where the task can fit
    // put it there
    for (Machine machine : machines) {
      boolean fit = machine.getTotalResAvail().greaterOrEqual(taskResources);
      if (!fit)
        continue;

      machine.assignTask(dag, taskId, taskDuration, taskResources, currentTime);

      return true;
    }
    return false;
  }

  // return: [Key: dagId -- Value: List<taskId>]
  public Map<Integer, List<Integer>> finishTasks(double currentTime) {

    // finish any task on this machine at the current time
    Map<Integer, List<Integer>> finishedTasks = new HashMap<Integer, List<Integer>>();

    System.out.println("Cluster starts collecting finihsTasks. Current Time: " + currentTime);
    for (Machine machine : machines) {
      Map<Integer, List<Integer>> finishedTasksMachine = machine.finishTasks(currentTime);

      for (Map.Entry<Integer, List<Integer>> entry : finishedTasksMachine
          .entrySet()) {
        int dagId = entry.getKey();
        List<Integer> tasksFinishedDagId = entry.getValue();
        if (finishedTasks.get(dagId) == null) {
          finishedTasks.put(dagId, new ArrayList<Integer>());
        }
        finishedTasks.get(dagId).addAll(tasksFinishedDagId);
      }
    }

    // update the currentTime with the earliestFinishTime on every machine
    return finishedTasks;
  }

  public List<Integer> failMachines(double currentTime) {
    List<Integer> failedList = new ArrayList<>();
    for (Machine m: machines) {
      if (m.failureNow(currentTime)) {
        failedList.add(m.getMachineId());
      }
    }
    return failedList;
  }

  public int killTasks(int dagId, Set<Integer> taskIds) {
    int numKilled = 0;
    for (Machine m : machines) {
      numKilled += m.killTasks(dagId, taskIds);
    }
    return numKilled;
  }

  // util classes //
  public boolean getExecMode() { return execMode; }
  public Machine getMachine(int machine_id) {
    return machines.get(machine_id);
  }

  public Collection<Machine> getMachines() {
    return machines;
  }
  public List<Machine> getMachinesList() {
    return machines;
  }

  public Resources getClusterMaxResAlloc() {
    Resources maxClusterResAvail = new Resources();
    for (Machine machine : machines) {
      maxClusterResAvail.sum(machine.maxResAlloc);
    }
    return maxClusterResAvail;
  }

  public Resources getClusterResAvail() {
    Resources clusterResAvail = new Resources();
    for (Machine machine : machines) {
      clusterResAvail.sum(machine.getTotalResAvail());
    }
    return clusterResAvail;
  }

  public double earliestFinishTime() {
    double earliestFinishTime = Double.MAX_VALUE;
    for (Machine machine : machines) {
      earliestFinishTime = Math.min(earliestFinishTime,
          machine.earliestFinishTime());
    }
    return earliestFinishTime;
  }

  public double earliestStartTime() {
    double earliestStartTime = Double.MAX_VALUE;
    for (Machine machine : machines) {
      earliestStartTime = Math.min(earliestStartTime,
          machine.earliestStartTime());
    }
    return earliestStartTime;
  }

  // end util classes //
}
