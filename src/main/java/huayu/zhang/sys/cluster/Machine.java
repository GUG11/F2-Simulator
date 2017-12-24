package huayu.zhang.sys.cluster;

import huayu.zhang.sys.datastructures.BaseDag;
import huayu.zhang.sys.datastructures.Resources;
import huayu.zhang.sys.datastructures.Task;
import huayu.zhang.sys.datastructures.StageDag;
import huayu.zhang.sys.simulator.Simulator;
import huayu.zhang.sys.utils.Utils;

import java.util.*;
import java.util.logging.Logger;

public class Machine {
  private static Logger LOG = Logger.getLogger(Machine.class.getName());

  int machineId;

  double failureRate_;
  double nextFailureTime_;
  Random rand_;

  boolean execMode;
  public Cluster cluster;

  // max capacity of this machine
  // default is 1.0 across all dimensions
  Resources maxResAlloc;
  Resources totalResAlloc;
  // map: expected completion time -> Task context
  public Map<Task, Double> runningTasks;
  private double diskVolume_;

  public Machine(int machineId, Resources size, double diskVolume, double failureRate, boolean execMode) {
    this.machineId = machineId;
    this.execMode = execMode;
    this.failureRate_ = failureRate;
    totalResAlloc = new Resources();
    assert size != null;
    maxResAlloc = Resources.clone(size);
    runningTasks = new HashMap<Task, Double>();
    this.diskVolume_ = diskVolume;
    this.rand_ = new Random();
    this.nextFailureTime_ = this.computeNextFailureTime(0, this.failureRate_);
    System.out.println("Initialize machine: "+machineId+" "+size + " failure rate:" + failureRate + " initial failure time: " + nextFailureTime_ + " execMode:" + execMode);
  }

  public double earliestFinishTime() {
    double earliestFinishTime = Double.MAX_VALUE;
    for (Double finishTime : runningTasks.values()) {
      earliestFinishTime = Math.min(earliestFinishTime, finishTime);
    }
    return earliestFinishTime;
  }

  public double earliestStartTime() {
    double earliestStartTime = Double.MAX_VALUE;
    for (Double startTime : runningTasks.values()) {
      earliestStartTime = Math.min(earliestStartTime, startTime);
    }
    return earliestStartTime;
  }

  public void assignTask(StageDag dag, int taskId, double taskDuration,
      Resources taskResources, double currentTime) {
    int dagId = dag.getDagId();
    // if task does not fit -> reject it
    boolean fit = getTotalResAvail().greaterOrEqual(taskResources);
    if (!fit) {
      LOG.warning("Dag " + dagId + " taskID " + taskId + " does not fit machine " + machineId);
      return;
    }

    // 1. update the amount of resources allocated
    totalResAlloc.sum(taskResources);

    // 2. compute the expected time for this task
    double expTaskComplTime = currentTime + taskDuration;
    Task t = new Task(dagId, taskId, taskDuration, taskResources);
    runningTasks.put(t, expTaskComplTime);

    LOG.info("Assign dag " + dagId + "stage " + dag.vertexToStage.get(taskId) + " task " + taskId + " to machine " + machineId + " expected to finish at: " + expTaskComplTime);
    // update resource allocated to the corresponding job
    // BaseDag dag = Simulator.getDag(dagId);
    dag.rsrcInUse.sum(dag.rsrcDemands(taskId));
    LOG.fine("Dag " + dagId + " resource usage: " + dag.rsrcInUse + "; machine resource usage:" + totalResAlloc);
  }

  // [dagId -> List<TaskId>]
  public Map<Integer, List<Integer>> finishTasks(double currentTime) {

    Map<Integer, List<Integer>> tasksFinished = new HashMap<Integer, List<Integer>>();

    Iterator<Map.Entry<Task, Double>> iter = runningTasks.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<Task, Double> entry = iter.next();

      if (entry.getValue() <= currentTime) {
        Task t = entry.getKey();
        totalResAlloc.subtract(t.resDemands);

        // update resource freed from corresponding job
        BaseDag dag = Simulator.getDag(t.dagId);
        dag.rsrcInUse.subtract(t.resDemands);
        LOG.info("Dag " + dag.getDagId() + ", stage " + ((StageDag)dag).vertexToStage.get(t.taskId) + ",Task " + t.taskId + " on machine " + machineId + " finished");
        LOG.fine("Dag " + dag.getDagId() + " resource usage: " + dag.rsrcInUse + "; machine resource usage:" + totalResAlloc);

        if (tasksFinished.get(t.dagId) == null) {
          tasksFinished.put(t.dagId, new ArrayList<Integer>());
        }
        tasksFinished.get(t.dagId).add(t.taskId);
        iter.remove();
      }
    }
    return tasksFinished;
  }

  public Resources getTotalResAvail() {
    return Resources.subtract(maxResAlloc, totalResAlloc);
  }

  public int getMachineId() {
    return this.machineId;
  }

  // exponential dist
  public double computeNextFailureTime(double currentTime, double rate) {
    double u = this.rand_.nextDouble();
    return Utils.round(Math.log(1 - u) / (-rate) + currentTime, 2);
  }

  public boolean failureNow(double currentTime) {
    if (nextFailureTime_< currentTime) {
      nextFailureTime_ = this.computeNextFailureTime(currentTime, this.failureRate_);
      // nextFailureTime_ = Double.MAX_VALUE;
      LOG.severe("Machine " + machineId + " fails at " + currentTime + "; next failure time: " + nextFailureTime_);
      return true;
    }
    return false;
  }

  public int killTasks(int dagId, Set<Integer> taskIds) {
    Set<Task> tasksToKill = new HashSet<>();   
    for (Map.Entry<Task, Double> entry: runningTasks.entrySet()) {
      Task t = entry.getKey();
      if (t.dagId == dagId && taskIds.contains(t.taskId)) {
        tasksToKill.add(t);
        BaseDag dag = Simulator.getDag(t.dagId);
        totalResAlloc.subtract(t.resDemands);
        dag.rsrcInUse.subtract(t.resDemands);
        LOG.warning("Kill dag " + dagId + " task " + t.taskId + "on machine " + machineId);
        LOG.fine("Dag " + dagId + " resource usage: " + dag.rsrcInUse + "; machine resource usage:" + totalResAlloc);
      }
    }
    runningTasks.keySet().removeAll(tasksToKill);
    return tasksToKill.size();
  }
}
