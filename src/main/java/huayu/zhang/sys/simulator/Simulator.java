package huayu.zhang.sys.simulator;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.logging.Logger;

import huayu.zhang.sys.F2.*;
import huayu.zhang.sys.cluster.Cluster;
import huayu.zhang.sys.datastructures.BaseDag;
import huayu.zhang.sys.datastructures.Stage;
import huayu.zhang.sys.datastructures.StageDag;
import huayu.zhang.sys.schedulers.InterJobScheduler;
import huayu.zhang.sys.schedulers.IntraJobScheduler;
import huayu.zhang.sys.simulator.Main.Globals;
import huayu.zhang.sys.utils.Configuration;
import huayu.zhang.sys.utils.DagParser;
import huayu.zhang.sys.utils.Randomness;
import huayu.zhang.sys.utils.Utils;
import java.util.*;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


// implement the timeline server
public class Simulator {
  private static Logger LOG = Logger.getLogger(Simulator.class.getName());

  // time
  private final double endTime_;
  private final double timeStep_;
  private double currentTime_;

  // job queues
  public static Queue<BaseDag> runnableJobs;
  public static Queue<BaseDag> runningJobs;
  public static Queue<BaseDag> completedJobs;

  private Cluster cluster_;

  public static Randomness r;

  int totalReplayedJobs;
  int lastCompletedJobs;

  public static InterJobScheduler interJobSched;
  public static IntraJobScheduler intraJobSched;

  // dag_id -> list of tasks
  public static Map<Integer, Set<Integer>> tasksToStartNow;

  // event queues
  private Queue<SpillEvent> spillEventQueue_;
  private Queue<ReadyEvent> readyEventQueue_;

  private DataService ds;
  private ExecuteService es;

  // file paths
  private final String pathToStatsOutput_;


  public Simulator(String pathToInputDagFile, String pathToConfigFile,
      String pathToStatsOutput) {
    Configuration config = new Configuration();
    DagParser dagParser = new DagParser();
    runnableJobs = dagParser.parseDAGSpecFile(pathToInputDagFile);
    config.parseConfigFile(pathToConfigFile);
    pathToStatsOutput_ = pathToStatsOutput;
    List<Double> quota = new ArrayList<Double>();
    for (BaseDag dag: runnableJobs) {
      quota.add(((StageDag)dag).getQuota());
    }
    spillEventQueue_ = new LinkedList<SpillEvent>();
    readyEventQueue_ = new LinkedList<ReadyEvent>();

    System.out.println("Print DAGs");
      for (BaseDag dag : runnableJobs) {
      ((StageDag) dag).viewDag();
    }

    totalReplayedJobs = runnableJobs.size();
    runningJobs = new LinkedList<BaseDag>();
    completedJobs = new LinkedList<BaseDag>();

    cluster_ = new Cluster(true);
    config.populateCluster(cluster_);
    interJobSched = new InterJobScheduler(cluster_, config.getSharePolicy());
    intraJobSched = new IntraJobScheduler(cluster_, config.getSchedPolicy());

    ds = new DataService(quota.stream().mapToDouble(v -> v).toArray(), config.getNumGlobalPart(), cluster_.getMachines().size(), config.getDataPolicy());
    es = new ExecuteService(cluster_, interJobSched, intraJobSched, runningJobs, completedJobs, config.getMaxPartitionsPerTask());

    endTime_ = Utils.round(config.getEndTime(), 2);
    timeStep_ = Utils.round(config.getTimeStep(), 2);

    tasksToStartNow = new TreeMap<Integer, Set<Integer>>();

    r = new Randomness();
  }

  public void simulate() {

    for (currentTime_ = 0; currentTime_ < endTime_; currentTime_ += timeStep_) {
      LOG.info("\n==== STEP_TIME:" + currentTime_
          + " ====\n");

      currentTime_ = Utils.round(currentTime_, 2);

      // fail machine
      List<Integer> failedList = cluster_.failMachines(currentTime_);
      ds.nodesFailure(failedList);    // update DS
      es.nodesFailure(failedList);    // update ES


      // tasksToStartNow.clear();

      // terminate any task if it can finish and update cluster available
      // resources

      // update jobs status with newly finished tasks
      boolean jobCompleted = es.finishTasks(spillEventQueue_, currentTime_);
      ds.removeCompletedJobs(completedJobs);

      LOG.info("runnable jobs: " + runnableJobs.size() + ", running jobs: " + runningJobs.size()
          + ", completed jobs: " + completedJobs.size());
      // stop condition
      if (stop()) {
        System.out.println("==== Final Report: Completed Jobs ====");
        TreeMap<Integer, Double> results = new TreeMap<Integer, Double>();
        double makespan = Double.MIN_VALUE;
        double average = 0.0;
        for (BaseDag dag : completedJobs) {
          double jobDur = (dag.jobEndTime - dag.jobStartTime);
        //  System.out.println("Dag:" + dag.dagId + " compl. time:"
        //      + (dag.jobEndTime - dag.jobStartTime));
          double dagDuration = (dag.jobEndTime - dag.jobStartTime);
          makespan = Math.max(makespan, dagDuration);
          average += dagDuration;
          results.put(dag.getDagId(), (dag.jobEndTime - dag.jobStartTime));
        }
        average /= completedJobs.size();
        System.out.println("---------------------");
        System.out.println("Avg. job compl. time:" + average);
        System.out.println("Makespan:" + makespan);
        for (Integer dagId : results.keySet()) {
          System.out.println(dagId+" "+results.get(dagId));
        }
        // output stats
        JSONObject jStats = es.generateStatistics();
        try (PrintWriter outStats = new PrintWriter(pathToStatsOutput_)) {
          outStats.println(jStats.toString());
          LOG.info("write statistics to " + pathToStatsOutput_);
        } catch (IOException e) {
          System.out.println(e);
        }
        break;
      }

      // handle jobs completion and arrivals
      boolean newJobArrivals = handleNewJobArrival();
      boolean needInterJobScheduling = newJobArrivals || jobCompleted;

      LOG.info("[Simulator]: jobCompleted:" + jobCompleted
        + " newJobArrivals:" + newJobArrivals);

      LOG.info("Spillevent queue size: " + spillEventQueue_.size());
      ds.receiveSpillEvents(spillEventQueue_, readyEventQueue_);
      LOG.info("Readyevent queue size: " + readyEventQueue_.size());
      es.receiveReadyEvents(needInterJobScheduling, readyEventQueue_);
      es.schedule(currentTime_);

      LOG.info("\n==== END STEP_TIME:" + currentTime_ + " ====\n");
    }
  }

  boolean stop() {
    return (runnableJobs.isEmpty() && runningJobs.isEmpty() && (completedJobs
        .size() == totalReplayedJobs));
  }

  boolean handleNewJobArrival() {
    // flag which specifies if jobs have inter-arrival times or starts at t=0
    System.out.println("handleNewJobArrival; currentTime:" + currentTime_);

    Set<BaseDag> newlyStartedJobs = new HashSet<BaseDag>();
    for (BaseDag dag : runnableJobs) {
      if (dag.getTimeArrival() <= currentTime_) {
        dag.jobStartTime = currentTime_;
        newlyStartedJobs.add(dag);
        System.out.println("Started job:" + dag.getDagId() + " at time:"
            + currentTime_);
      }
    }
    // clear the datastructures
    runnableJobs.removeAll(newlyStartedJobs);
    runningJobs.addAll(newlyStartedJobs);

    return true;
  }

  boolean handleNewJobCompleted() {
    int currCompletedJobs = completedJobs.size();
    if (lastCompletedJobs < currCompletedJobs) {
      lastCompletedJobs = currCompletedJobs;
      return true;
    }
    return false;
  }

  public static StageDag getDag(int dagId) {
    for (BaseDag dag : Simulator.runningJobs) {
      if (dag.getDagId() == dagId) {
        return (StageDag) dag;
      }
    }
    return null;
  }

  public Cluster getCluster() { return cluster_; }
  public double getCurrentTime() { return currentTime_; }
}
