package huayu.zhang.sys.datastructures;

import java.util.logging.Logger;

import huayu.zhang.sys.cluster.Cluster;
import huayu.zhang.sys.simulator.Main.Globals;
import huayu.zhang.sys.simulator.Simulator;
import huayu.zhang.sys.utils.Interval;
import huayu.zhang.sys.utils.Utils;
import java.util.*;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


public class StageDag extends BaseDag {

  private static Logger LOG = Logger.getLogger(StageDag.class.getName());
  private double quota_;
  private double[] inputKeySizes_;

  public Map<String, Stage> stages;
  public Map<Integer, String> vertexToStage;  // <vertexId (taskID), stageName vertexId in>
  public Map<String, Double> stageCPLength;

  private Set<String> runnableStages_;
  private Set<String> runningStages_;
  private Set<String> finishedStages_;
  private List<String> killedStages_;

  private List<Integer> killedTasks_;

  private Map<Integer, Integer> taskToMachine_; // assign certain task to machine. set -1 if no preference

  public Map<Integer, Task> adjustedTaskDemands = null;

  public StageDag(int id, double quota, double[] inputKeySizes, double arrival) {
    super(id, arrival);
    stages = new HashMap<String, Stage>();
    quota_ = quota;
    runnableStages_ = new HashSet<>();
    runningStages_ = new HashSet<>();
    finishedStages_ = new HashSet<>();
    killedStages_ = new LinkedList<>();
    killedTasks_ = new LinkedList<>();
    inputKeySizes_ = inputKeySizes;
    taskToMachine_ = new HashMap<>();
  }

  public double getQuota() {return quota_; }

  public double[] getInputKeySize() {return inputKeySizes_; }

  public boolean isRunnableStage(String name) { return runnableStages_.contains(name); }
  public boolean isRunningStage(String name) { return runningStages_.contains(name); }
  public boolean isFinishedStage(String name) { return finishedStages_.contains(name); }

  public boolean isRoot(String name) {
    return stages.containsKey(name) && stages.get(name).parents.isEmpty();
  }

  public String onDataLoss(Set<String> availableStages, Set<Integer> taskToKill) {
      // assume linear graph
      List<Integer> taskNotToRun = new LinkedList<>();
      boolean kill = false;
      String activeStage = null;
      if (!runnableStages_.isEmpty()) {
        activeStage = runnableStages_.iterator().next();
      } else if (!runningStages_.isEmpty()) {
        activeStage = runningStages_.iterator().next();
      }
      LOG.info("Dag " + dagId + ": Current running/runable stage: " + activeStage);
      if (activeStage != null && !isRoot(activeStage)) {
        String parentStage = stages.get(activeStage).parents.keySet().iterator().next();
        if (!availableStages.contains(parentStage)) {
          LOG.info("Dag " + dagId + ": Current running/runable's parent stage: " + parentStage + " is unavailable");
          // find tasks
          for (Map.Entry<Integer, String> taskStage: vertexToStage.entrySet()) {
            int taskId = taskStage.getKey();
            String stageName = taskStage.getValue();
            if (activeStage.equals(stageName)) {
              if (this.runnableTasks.contains(taskId)) {
                kill = true;    // only kill the stage if there is a runnable task
                taskNotToRun.add(taskId);
              } else if (this.runningTasks.contains(taskId)) {
                taskToKill.add(taskId);
              }
            }
          }
        }
        if (!kill) {
          taskToKill.clear();
          activeStage = null;
          LOG.info("Dag " + dagId + ": On loss of data of stages(avail): " + availableStages + ", nothing happens");
        } else {
          runningTasks.removeAll(taskToKill);
          runnableTasks.removeAll(taskNotToRun);   // linked list (need optimize)
          killStage(activeStage);
          killedTasks_.addAll(taskToKill);
          // find the nearest available ancestors
          while (!availableStages.contains(parentStage)) {
            activeStage = parentStage;
            finishedStages_.remove(activeStage);
            Set<String> parentSet = stages.get(activeStage).parents.keySet();
            if (parentSet.isEmpty()) break;
            parentStage = parentSet.iterator().next();
          }
          LOG.warning("Dag " + dagId + ": On loss of data of stages(avail): " + availableStages +
              ", kill running tasks:" + taskToKill + 
              ", remove runnable tasks:" + taskNotToRun + ", reschedule ancestor stage: "
              + activeStage + ". Current runnable tasks:" + runnableTasks + 
              ", current running stages: " + runningStages_
              + ", current runnable stages: " + runnableStages_ 
              + ", finished stages: " + finishedStages_ 
              + ", current killed stages: " + killedStages_);
        }
      }
      if (!kill) activeStage = null;
      return activeStage; // null means no need to reschedule
  }

  public void killStage(String name) {
    assert (isRunningStage(name) || isRunningStage(name));
    LOG.warning("Dag " + dagId + ": Kill stage: " + name);
    if (isRunnableStage(name)) {
      runnableStages_.remove(name);
    } else if (isRunningStage(name)) {
      runningStages_.remove(name);
    }
    killedStages_.add(name);
  }

  public void addRunnableStage(String name) {
    if (stages.containsKey(name)) {
      runnableStages_.add(name);
    } else {
      LOG.severe(name + " is not a valid stage in the current job (" + dagId + ")");
    }
  }

  public void moveRunnableToRunning(String name) {
    if (runnableStages_.contains(name)) {
      runnableStages_.remove(name);
      runningStages_.add(name);
    } else {
      LOG.severe(name + " is not a valid runnable stage in the current job (" + this.dagId + ")");
    }
  }

  public void moveRunningToFinish(String name) {
    if (runningStages_.contains(name)) {
      runningStages_.remove(name);
      finishedStages_.add(name);
    } else {
      LOG.severe(name + " is not a valid running stage in the current job (" + this.dagId + ")");
    }
  }

  public Set<String> updateRunnableStages() {
    Set<String> newRunnableSet = new HashSet<>();
    for (Map.Entry<String, Stage> entry: stages.entrySet()) {
      String name = entry.getKey();
      if (runnableStages_.contains(name) ||
          runningStages_.contains(name) ||
          finishedStages_.contains(name)) continue;
      if (finishedStages_.containsAll(entry.getValue().parents.keySet())) {
        runnableStages_.add(name);
        newRunnableSet.add(name);
      }
    }
    return newRunnableSet;
  }

  public Set<String> setInitiallyRunnableStages() {
    LOG.info("set initially runnable stages for dag " + dagId);
    Set<String> newRunnableSet = new HashSet<>();
    for (Map.Entry<String, Stage> entry: stages.entrySet()) {
      String name = entry.getKey();
      if (!runnableStages_.contains(name) &&
          !runningStages_.contains(name) &&
          !finishedStages_.contains(name) &&
          entry.getValue().parents.isEmpty()) {
        newRunnableSet.add(name);
        runnableStages_.add(name);
      }
    }
    return newRunnableSet;
  }

  // view dag methods //
  public void viewDag() {
    System.out.println("\n == DAG: " + this.dagId + " ==");

    for (Stage stage : stages.values()) {
      System.out.print("Stage: " + stage.name + ", duration:" + stage.vDuration + ", ");
      System.out.println(stage.vDemands);

      System.out.print("Maximum Parallel Tasks:" + stage.getNumTasks());

      System.out.print("  Parents: ");
      for (String parent : stage.parents.keySet())
        System.out.print(parent + ", ");
      System.out.println();
    }

    System.out.println("== CP ==");
    System.out.println(stageCPLength);
  }

  public void populateParentsAndChildrenStructure(String stage_src,
      String stage_dst, String comm_pattern) {

    if (!stages.containsKey(stage_src) || !stages.containsKey(stage_dst)) {
      LOG.severe("A stage entry for " + stage_src + " or " + stage_dst
          + " should be already inserted !!!");
      return;
    }
    if (stages.get(stage_src).children.containsKey(stage_dst)) {
      LOG.severe("An edge b/w " + stage_src + " and " + stage_dst
          + " is already present.");
      return;
    }
    Dependency d = new Dependency(stage_src, stage_dst, comm_pattern);
    //    stages.get(stage_src).vids, stages.get(stage_dst).vids);

    stages.get(stage_src).children.put(stage_dst, d);
    stages.get(stage_dst).parents.put(stage_src, d);
  }

  public void addRunnableTask(int taskId, String stageName, int machineId) {
    this.runnableTasks.add(taskId);
    this.vertexToStage.put(taskId, stageName);
    this.taskToMachine_.put(taskId, machineId);
  }

  public int getAssignedMachine(int taskId) {
    return taskToMachine_.get(taskId);
  }
  // end read dags from file //

  // DAG traversals //
  public void setCriticalPaths() {
    if (stageCPLength == null) {
      stageCPLength = new HashMap<>();
    }
    for (String stageName : stages.keySet()) {
      longestCriticalPath(stageName);
    }
  }

  public double getMaxCP() {
    return Collections.max(stageCPLength.values());
  }

  public double longestCriticalPath(String stageName) {
    if (stageCPLength != null && stageCPLength.containsKey(stageName)) {
      return stageCPLength.get(stageName);
    }

    if (stageCPLength == null) {
      stageCPLength = new HashMap<String, Double>();
    }

    double maxChildCP = Double.MIN_VALUE;
    // String stageName = this.vertexToStage.get(taskId);

    Set<String> childrenStages = stages.get(stageName).children.keySet();
    // System.out.println("Children: "+children);
    if (childrenStages.size() == 0) {
      maxChildCP = 0;
    } else {
      for (String chStage : childrenStages) {
        double childCP = longestCriticalPath(chStage);
        if (maxChildCP < childCP) {
          maxChildCP = childCP;
        }
      }
    }

    double cp = maxChildCP + stages.get(stageName).vDuration;
    if (!stageCPLength.containsKey(stageName)) {
      stageCPLength.put(stageName, cp);
    }

    return stageCPLength.get(stageName);
  }

  public void printCPLength() {
    System.out.println("DagID:" + dagId + " critical path lengths");
    for (Map.Entry<String, Double> entry : stageCPLength.entrySet()) {
      System.out.println("task ID:" + entry.getKey() + ", cp length:" + entry.getValue());
    }
  }

  public double getCPLength(int taskId) {
    return stageCPLength.get(this.vertexToStage.get(taskId));
  }


  @Override
  public Resources rsrcDemands(int taskId) {
    if (adjustedTaskDemands != null && adjustedTaskDemands.get(taskId) != null) {
      return adjustedTaskDemands.get(taskId).resDemands;
    }
    return stages.get(vertexToStage.get(taskId)).rsrcDemandsPerTask();
  }

  @Override
  public double duration(int taskId) {
    return stages.get(vertexToStage.get(taskId)).vDuration;
  }

  @Override
  public Set<Integer> allTasks() {
    return vertexToStage.keySet();
  }

  public int numTotalTasks() {
    int n = 0;
    for (Stage stage : stages.values()) {
      n += stage.getNumTasks();
    }
    return n;
  }

  public Set<Integer> remTasksToSchedule() {
    Set<Integer> allTasks = new HashSet<Integer>(vertexToStage.keySet());
    Set<Integer> consideredTasks = new HashSet<Integer>(this.finishedTasks);
    consideredTasks.addAll(this.runningTasks);
    allTasks.removeAll(consideredTasks);
    return allTasks;
  }

  public Resources totalResourceDemand() {
    Resources totalResDemand = new Resources(0.0);
    for (Stage stage : stages.values()) {
      totalResDemand.sum(stage.totalWork());
    }
    return totalResDemand;
  }

  public Resources totalWorkInclDur() {
    Resources totalResDemand = new Resources(0.0);
    for (Stage stage : stages.values()) {
      totalResDemand.sum(stage.totalWork());
    }
    return totalResDemand;
  }

  @Override
  public double totalWorkJob() {
    double scoreTotalWork = 0;
    for (Stage stage : stages.values()) {
      scoreTotalWork += stage.stageContribToSrtfScore(new HashSet<Integer>());
    }
    return scoreTotalWork;
  }

  // should decrease only the resources allocated in the current time quanta
  public Resources currResShareAvailable() {
    Resources totalShareAllocated = Resources.clone(this.rsrcQuota);

    for (int task: runningTasks) {
      Resources rsrcDemandsTask = rsrcDemands(task);
      totalShareAllocated.subtract(rsrcDemandsTask);
    }
    LOG.finest("Dag: " + dagId + " compute resource available." + 
    " quota: " + rsrcQuota + "available: " + totalShareAllocated + "running tasks:" + runningTasks.size());
    totalShareAllocated.normalize();  // change <0 to 0
    return totalShareAllocated;
  }

  public JSONObject generateStatistics() {
    JSONObject jObj = new JSONObject();
    jObj.put("time", Utils.round(jobEndTime - jobStartTime, 2));
    jObj.put("num_killed_tasks", killedTasks_.size());
    jObj.put("num_finished_tasks", finishedTasks.size());
    return jObj;
  }
}
