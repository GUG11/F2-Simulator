package huayu.zhang.sys.datastructures;

import huayu.zhang.sys.utils.Interval;

import java.util.*;

public abstract class BaseDag {

  protected int dagId;
  private double timeArrival_;

  public abstract Resources rsrcDemands(int task_id);

  public abstract double duration(int task_id);

  public abstract Set<Integer> allTasks();

  public Resources rsrcQuota;
  public Resources rsrcInUse;

  public LinkedHashSet<Integer> runnableTasks;
  public LinkedHashSet<Integer> runningTasks;
  public LinkedHashSet<Integer> finishedTasks;

  public LinkedHashSet<Integer> launchedTasksNow;

  public double jobStartTime, jobEndTime;
  public double jobExpDur;

  // keep track remaining time from current time given some share
  public double timeToComplete;

  public Map<Integer, Task> idToTask;

  public BaseDag(int id, double arrival) {
    this.dagId = id;
    this.timeArrival_ = arrival;

    rsrcQuota = new Resources();
    rsrcInUse = new Resources();

    runnableTasks = new LinkedHashSet<Integer>();
    runningTasks = new LinkedHashSet<Integer>();
    finishedTasks = new LinkedHashSet<Integer>();
    idToTask = new HashMap<>();

    launchedTasksNow = new LinkedHashSet<Integer>();
  }

  public Resources currResDemand() {
    Resources usedRes = new Resources(0.0);
    for (int taskId : runningTasks) {
      usedRes.sum(rsrcDemands(taskId));
    }
    return usedRes;
  }

  public int getDagId() { return this.dagId; }
  public double getTimeArrival() { return timeArrival_; }

    public void printLaunchedTasks() {
    System.out.print("DagID:" + dagId + " launched tasks now:");
    launchedTasksNow.stream().forEach(taskID -> System.out.print(taskID + ","));
    System.out.println("");
  }
}
