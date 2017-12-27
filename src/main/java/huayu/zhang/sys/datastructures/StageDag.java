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

  private Map<String, Stage> stages_;
  private Map<Integer, String> taskToStageMap_;  // <vertexId (taskID), stageName vertexId in>
  private Map<String, Double> stageCPLength_;

  private Set<String> runnableStages_;
  private Set<String> runningStages_;
  private Set<String> finishedStages_;
  private List<String> killedStages_;

  private List<Integer> killedTasks_;

  private Map<Integer, Integer> taskToMachine_; // assign certain task to machine. set -1 if no preference

  // related to Data Service
  

  public StageDag(int id, double quota, double[] inputKeySizes, double arrival) {
    super(id, arrival);
    stages_ = new HashMap<String, Stage>();
    quota_ = quota;
    runnableStages_ = new HashSet<>();
    runningStages_ = new HashSet<>();
    finishedStages_ = new HashSet<>();
    killedStages_ = new LinkedList<>();
    killedTasks_ = new LinkedList<>();
    inputKeySizes_ = inputKeySizes;
    taskToMachine_ = new HashMap<>();
    taskToStageMap_ = new HashMap<>();
  }

  public double getQuota() {return quota_; }

  public double[] getInputKeySize() {return inputKeySizes_; }

  public boolean isRunnableStage(String name) { return runnableStages_.contains(name); }
  public boolean isRunningStage(String name) { return runningStages_.contains(name); }
  public boolean isFinishedStage(String name) { return finishedStages_.contains(name); }

  public boolean isRoot(String name) {
    return stages_.containsKey(name) && stages_.get(name).isRoot();
  }

  public boolean isLeaf(String name) {
    return stages_.containsKey(name) && stages_.get(name).isLeaf();
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
        String parentStage = stages_.get(activeStage).getParentStages().iterator().next();
        if (!availableStages.contains(parentStage)) {
          LOG.info("Dag " + dagId + ": Current running/runable's parent stage: " + parentStage + " is unavailable");
          // find tasks
          for (Map.Entry<Integer, String> taskStage: taskToStageMap_.entrySet()) {
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
            Set<String> parentSet = stages_.get(activeStage).getParentStages();
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
    if (stages_.containsKey(name)) {
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
    for (Map.Entry<String, Stage> entry: stages_.entrySet()) {
      String name = entry.getKey();
      if (runnableStages_.contains(name) ||
          runningStages_.contains(name) ||
          finishedStages_.contains(name)) continue;
      if (finishedStages_.containsAll(entry.getValue().getParentStages())) {
        runnableStages_.add(name);
        newRunnableSet.add(name);
      }
    }
    return newRunnableSet;
  }

  public Set<String> setInitiallyRunnableStages() {
    LOG.info("set initially runnable stages for dag " + dagId);
    Set<String> newRunnableSet = new HashSet<>();
    for (Map.Entry<String, Stage> entry: stages_.entrySet()) {
      String name = entry.getKey();
      if (!runnableStages_.contains(name) &&
          !runningStages_.contains(name) &&
          !finishedStages_.contains(name) &&
          entry.getValue().getParentStages().isEmpty()) {
        newRunnableSet.add(name);
        runnableStages_.add(name);
      }
    }
    return newRunnableSet;
  }

  // view dag methods //
  public void viewDag() {
    System.out.println("\n == DAG: " + this.dagId + " ==");

    for (Stage stage : stages_.values()) {
      System.out.print("Stage: " + stage.getName() + ", duration:" + stage.getDuration() + ", ");
      System.out.println(stage.getDemands());

      System.out.print("Maximum Parallel Tasks:" + stage.getNumTasks());

      System.out.print("  Parents: ");
      for (String parent : stage.getParentStages())
        System.out.print(parent + ", ");
      System.out.println();
    }

    System.out.println("== CP ==");
    System.out.println(stageCPLength_);
  }

  public void populateParentsAndChildrenStructure(String stage_src,
      String stage_dst) {

    if (!stages_.containsKey(stage_src) || !stages_.containsKey(stage_dst)) {
      LOG.severe("A stage entry for " + stage_src + " or " + stage_dst
          + " should be already inserted !!!");
      return;
    }
    if (stages_.get(stage_src).isParentOf(stage_dst)) {
      LOG.severe("An edge b/w " + stage_src + " and " + stage_dst
          + " is already present.");
      return;
    }
    stages_.get(stage_src).addChildStage(stage_dst);
    stages_.get(stage_dst).addParentStage(stage_src);
  }

  public void addRunnableTask(int taskId, String stageName, int machineId) {
    this.runnableTasks.add(taskId);
    this.taskToStageMap_.put(taskId, stageName);
    this.taskToMachine_.put(taskId, machineId);
  }

  public int getAssignedMachine(int taskId) {
    return taskToMachine_.get(taskId);
  }
  // end read dags from file //

  // DAG traversals //
  public void setCriticalPaths() {
    if (stageCPLength_ == null) {
      stageCPLength_ = new HashMap<>();
    }
    for (String stageName : stages_.keySet()) {
      longestCriticalPath(stageName);
    }
  }

  public double getMaxCP() {
    return Collections.max(stageCPLength_.values());
  }

  public double longestCriticalPath(String stageName) {
    if (stageCPLength_ != null && stageCPLength_.containsKey(stageName)) {
      return stageCPLength_.get(stageName);
    }

    if (stageCPLength_ == null) {
      stageCPLength_ = new HashMap<String, Double>();
    }

    double maxChildCP = Double.MIN_VALUE;
    // String stageName = this.taskToStageMap_.get(taskId);

    Set<String> childrenStages = stages_.get(stageName).getChildStages();
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

    double cp = maxChildCP + stages_.get(stageName).getDuration();
    if (!stageCPLength_.containsKey(stageName)) {
      stageCPLength_.put(stageName, cp);
    }

    return stageCPLength_.get(stageName);
  }

  public void printCPLength() {
    System.out.println("DagID:" + dagId + " critical path lengths");
    for (Map.Entry<String, Double> entry : stageCPLength_.entrySet()) {
      System.out.println("task ID:" + entry.getKey() + ", cp length:" + entry.getValue());
    }
  }

  public double getCPLength(int taskId) {
    return stageCPLength_.get(this.taskToStageMap_.get(taskId));
  }


  @Override
  public Resources rsrcDemands(int taskId) {
    return stages_.get(taskToStageMap_.get(taskId)).rsrcDemandsPerTask();
  }

  @Override
  public double duration(int taskId) {
    return stages_.get(taskToStageMap_.get(taskId)).getDuration();
  }

  @Override
  public Set<Integer> allTasks() {
    return taskToStageMap_.keySet();
  }

  public int numTotalTasks() {
    int n = 0;
    for (Stage stage : stages_.values()) {
      n += stage.getNumTasks();
    }
    return n;
  }

  public Resources totalResourceDemand() {
    Resources totalResDemand = new Resources(0.0);
    for (Stage stage : stages_.values()) {
      totalResDemand.sum(stage.totalWork());
    }
    return totalResDemand;
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

  public String findStageByTaskID(int taskId) {
    return this.taskToStageMap_.get(taskId);
  }

  public Stage getStageByName(String stageName) {
    return this.stages_.get(stageName);
  }

  public Stage addStage(String stageName, Stage stage) {
    return this.stages_.put(stageName, stage);
  }
}
