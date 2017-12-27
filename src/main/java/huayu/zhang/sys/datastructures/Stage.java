package huayu.zhang.sys.datastructures;

import huayu.zhang.sys.utils.Interval;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Stage {
  private String name_;

  private Set<Integer> taskIds_;

  private int numTasks_;    // maximum number of tasks that can be parallelized
  private double outinRatio_;
  private double duration_;
  private Resources demands_;

  private Set<String> parents_, children_;

  public Stage(String name, int numTasks, double duration,
      double[] resources, double outinRatio) {
    this.name_ = name;
    numTasks_ = numTasks;
    outinRatio_ = outinRatio;

    parents_ = new HashSet<>();
    children_ = new HashSet<>();

    taskIds_ = new HashSet<>();

    duration_ = duration;
    demands_ = new Resources(resources);

    // System.out.println("New Stage" + this.name + "," + this.id + "," + this.vids + ", Duration:" + duration_ + ",Demands" + demands_); 
  }

  public Resources rsrcDemandsPerTask() { return demands_; }

  public void addTaskId(int tid) {
    taskIds_.add(tid);
  }

  public int getNumTasks() { return numTasks_; }
  public String getName() { return name_; }
  public double getDuration() { return duration_; }
  public Resources getDemands() { return demands_; }

  public boolean addParentStage(String name) { return parents_.add(name); }
  public boolean addChildStage(String name) { return children_.add(name); }

  public boolean isParentOf(String name) { return children_.contains(name); }
  public boolean isChildOf(String name) { return parents_.contains(name); }

  public Set<String> getParentStages() { return parents_; }
  public Set<String> getChildStages() { return children_; }

  public boolean isRoot() { return parents_.isEmpty(); }
  public boolean isLeaf() { return children_.isEmpty(); }

  public double getOutinRatio() { return outinRatio_; }

  public Resources totalWork() {
    Resources totalWork = Resources.clone(demands_);
    totalWork.multiply(numTasks_);
    return totalWork;
  }
}
