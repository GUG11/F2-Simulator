package huayu.zhang.sys.F2;

import huayu.zhang.sys.cluster.Cluster;
import huayu.zhang.sys.cluster.Machine;
import huayu.zhang.sys.datastructures.*;
import huayu.zhang.sys.schedulers.InterJobScheduler;
import huayu.zhang.sys.schedulers.IntraJobScheduler;
import huayu.zhang.sys.simulator.Simulator;

import javax.crypto.Mac;
import java.util.*;
import java.util.logging.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class ExecuteService {

  private static Logger LOG = Logger.getLogger(StageDag.class.getName());
  private Cluster cluster_;
  private InterJobScheduler interJobScheduler_;
  private IntraJobScheduler intraJobScheduler_;
  private Queue<BaseDag> runningJobs_;
  private Queue<BaseDag> completedJobs_;
  private int nextId_;
  private int maxPartitionsPerTask_;

  private Map<Integer, Map<Integer, Double>> taskOutputs_;  // (taskId, (key, size))
  private Map<Integer, Map<String, Integer>> dagStageNumTaskMap_;    // (dagId, stageName, num of tasks)
  private Map<Integer, Map<String, Map<Integer, Map<Integer, Partition>>>> availablePartitions_; // (dagId, stageName, machineId, paritionId, partition)
  private Map<Integer, Set<String>> dagRunnableStagesMap_;

  // stats
  private int numFailureEvents_;

  public ExecuteService(Cluster cluster, InterJobScheduler interJobScheduler,
                        IntraJobScheduler intraJobScheduler,
                        Queue<BaseDag> runningJobs, Queue<BaseDag> completedJobs, int maxPartitionsPerTask) {
    cluster_ = cluster;
    interJobScheduler_ = interJobScheduler;
    intraJobScheduler_ = intraJobScheduler;
    runningJobs_ = runningJobs;
    completedJobs_ = completedJobs;
    maxPartitionsPerTask_ = maxPartitionsPerTask;
    nextId_ = 0;
    taskOutputs_ = new HashMap<>();
    availablePartitions_ = new HashMap<>();
    dagStageNumTaskMap_ = new HashMap<>();
    dagRunnableStagesMap_ = new HashMap<>();
    numFailureEvents_ = 0;
  }

  private void addPartition(int dagId, String stageName, int machineId, int pid, Partition partition) {
    Map<String, Map<Integer, Map<Integer, Partition>>> smpp = null;
    Map<Integer, Map<Integer, Partition>> mpp = null;
    Map<Integer, Partition> pp = null;
    if (!availablePartitions_.containsKey(dagId)) {
      availablePartitions_.put(dagId, new HashMap<>());
    }
    smpp = availablePartitions_.get(dagId);
    if (!smpp.containsKey(stageName)) {
      smpp.put(stageName, new HashMap<>());
    }
    mpp = smpp.get(stageName);
    if (!mpp.containsKey(machineId)) {
      mpp.put(machineId, new HashMap<>());
    }
    pp = mpp.get(machineId);
    pp.put(pid, partition);
  }

  private void removePartitions(int dagId, String stageName) {
    Map<String, Map<Integer, Map<Integer, Partition>>> smpp = availablePartitions_.get(dagId);
    if (smpp != null) {
      smpp.remove(stageName);
    }
  }

  public void printAvailablePartition(int dagId, String stageName) {
    Map<Integer, Map<Integer, Partition>> macPart = availablePartitions_.get(dagId).get(stageName);
    for (Map.Entry<Integer, Map<Integer, Partition>> mpEntry : macPart.entrySet()) {
      int machineId = mpEntry.getKey();
      Map<Integer, Partition> partMap = mpEntry.getValue();
      System.out.print(String.format("Available partitions for dag %d, stage %s on machine %d, ", dagId, stageName, machineId)); 
      System.out.println(partMap.keySet().size() + " paritions: " + partMap.keySet());
    }
  }

  public void receiveReadyEvents(boolean needInterJobScheduling, Queue<ReadyEvent> readyEventQueue) {
    Set<String> newRunnableStageNameSet = null;
    int dagId = -1;
    int taskId = -1;
    if(needInterJobScheduling) {
      interJobScheduler_.schedule(cluster_);
    }
    LOG.info("Running jobs size:" + runningJobs_.size());
    for (BaseDag dag: runningJobs_) {
      StageDag rDag = (StageDag)dag;
      if (!dagRunnableStagesMap_.containsKey(rDag.getDagId())) {
        dagRunnableStagesMap_.put(rDag.getDagId(), new HashSet<>());
      }
      dagRunnableStagesMap_.get(rDag.getDagId()).addAll(rDag.setInitiallyRunnableStages());      
    }
    ReadyEvent readyEvent = readyEventQueue.poll();
    while(readyEvent != null) {
      LOG.info("Receive a ready event: " + readyEvent.toString());
      dagId = readyEvent.getDagId();
      if (!dagRunnableStagesMap_.containsKey(dagId)) {
        dagRunnableStagesMap_.put(dagId, new HashSet<>());
      }
      receiveReadyEvent(readyEvent, dagRunnableStagesMap_.get(dagId));
      readyEvent = readyEventQueue.poll();
    }
    LOG.info("Runnable dag stages: " + dagRunnableStagesMap_);

    // TODO: collect data and assign tasks
    //     1. handle data locaility
    //     2. compute the output key sizes
    for (Map.Entry<Integer, Set<String>> entry: dagRunnableStagesMap_.entrySet()) {
      dagId = entry.getKey();
      StageDag dag = getDagById(dagId);
      newRunnableStageNameSet = entry.getValue();
      Map<Integer, Double> taskOutput = null;
      int totalNumTasks = 0;
      for(String stageName : newRunnableStageNameSet) {
        dag.addRunnableStage(stageName);
        Stage stage = dag.stages.get(stageName);
        int numTasks = stage.getNumTasks();
        totalNumTasks = numTasks;
        if (stage.parents.isEmpty()) {    // start stages
          double[] keySizesPerTask = Arrays.stream(dag.getInputKeySize()).map(v -> v / stage.getNumTasks()).toArray();
          while (0 < numTasks--) {
            taskId = nextId_;
            nextId_++;
            // compute output data size
            if (!taskOutputs_.containsKey(taskId)) {
              taskOutputs_.put(taskId, new HashMap<>());
            }
            taskOutput = taskOutputs_.get(taskId);
            for (int i = 0; i < keySizesPerTask.length; i++) {
              taskOutput.put(i, keySizesPerTask[i] * stage.getOutinRatio());
            }
            // add runnable task
            LOG.info("Dag " + dagId + "Stage " + stageName + "add runnable task:"+ taskId);
            dag.addRunnableTask(taskId, stageName, -1);
          }
        } else {   // non-start stages
          totalNumTasks = 0;
          // System.out.println("availablePartitions_=" + availablePartitions_);
          // TODO: multiple parents
          Set<String> parents = dag.stages.get(stageName).parents.keySet();
          assert parents.size() == 1;
          String parent = parents.iterator().next();
          System.out.println("dagId=" + dagId + ", stageName=" + stageName + ", parent(s)=" + parent);
          printAvailablePartition(dagId, parent);
          Map<Integer, Map<Integer, Partition>> machinePartMap = availablePartitions_.get(dagId).get(parent);
          for (Map.Entry<Integer, Map<Integer, Partition>> mchPart: machinePartMap.entrySet()) {
            int machineId = mchPart.getKey();
            Map<Integer, Partition> partMap = mchPart.getValue();
            int count = 0;
            for (Map.Entry<Integer, Partition> partkv: partMap.entrySet()) {
              if (count % maxPartitionsPerTask_ == 0) {
                taskId = nextId_;
                nextId_++;
                totalNumTasks++;
                if (!taskOutputs_.containsKey(taskId)) {
                  taskOutputs_.put(taskId, new HashMap<>());
                }
                taskOutput = taskOutputs_.get(taskId);
                LOG.info("Dag " + dagId + "Stage " + stageName + "add runnable task:"+ taskId);
                dag.addRunnableTask(taskId, stageName, machineId);
              }
              count++;
              Partition pt = partkv.getValue();
              Map<Integer, Map<Integer, Double>> data = pt.getData();   // machine, key, size
              assert data.size() == 1 && data.containsKey(machineId);  // already aggregated
              Map<Integer, Double> ksMap = data.get(machineId);
              for (Map.Entry<Integer, Double> ksPair: ksMap.entrySet()) {
                taskOutput.put(ksPair.getKey(), ksPair.getValue() * stage.getOutinRatio());
              }
            }
          }
        }
        if (!dagStageNumTaskMap_.containsKey(dagId)) {
          dagStageNumTaskMap_.put(dagId, new HashMap<>());
        }
        LOG.info("Dag: " + dagId + " Stage: " + stageName + ", number of runnable tasks=" + totalNumTasks);
        dagStageNumTaskMap_.get(dagId).put(stageName, totalNumTasks);
      }
    }
    dagRunnableStagesMap_.clear();
  }

  private void receiveReadyEvent(ReadyEvent readyEvent, Set<String> newRunnableStageNameSet) {
    int dagId = readyEvent.getDagId();
    BaseDag dag = getDagById(dagId);
    //fetch data from the partition of this readyEvent
    String stageName = readyEvent.getStageName();
    Partition partition = readyEvent.getPartition();

    List<Integer> machines = partition.getMachinesInvolved();
    double max = -1;
    int id = -1;
    for(Integer machineId : machines) {
      double cur = partition.getPartitionSizeOnMachine(machineId);
      if(max < cur) {
        max = cur;
        id = machineId;
      }
    }

    //copy data to a single node if more than 1 machine is data holder
    if(machines.size() > 1) {
      partition.aggregateKeyShareToSingleMachine(id, machines);
    }
    if (-1 < id) {
      this.addPartition(dagId, stageName, id, readyEvent.getPartitionId(), partition);
    } else {
      LOG.severe("Empty readyEvent: " + readyEvent);
    }

    if(partition.isLastPartReady()) {
      newRunnableStageNameSet.addAll(updateRunnable(stageName, dag));
    }
  }

  Set<String> updateRunnable(String stageName, BaseDag dag) {
    StageDag stageDag = (StageDag) dag;
    stageDag.moveRunningToFinish(stageName);
    Set<String> result = stageDag.updateRunnableStages();
    return result;
  }

  public void schedule() {
    LOG.info("running jobs: " + runningJobs_.size());
    for (BaseDag dag: runningJobs_) {
      LOG.info("schedule dag: " + dag.dagId);
      intraJobScheduler_.schedule((StageDag) dag);
    }
  }

  private void schedule(int dagId) {
    BaseDag dag = getDagById(dagId);
    if(dag == null) {
      System.out.println("Error: Dag is not running any more when trying to schedule");
      return;
    }
    intraJobScheduler_.schedule((StageDag) dag);
  }

  private StageDag getDagById(int dagId) {
    BaseDag dag = null;
    for(BaseDag e : runningJobs_) {
      if(e.getDagId() == dagId) {
        dag = e;
        break;
      }
    }
    return (StageDag)dag;
  }

  public boolean finishTasks(Queue<SpillEvent> spillEventQueue) {
    boolean jobCompleted = false;
    Map<Integer, List<Integer>> finishedTasks = cluster_.finishTasks();
    for (Map.Entry<Integer, List<Integer>> entry: finishedTasks.entrySet()) {
      int dagId = entry.getKey();
      List<Integer> finishedTasksPerDag = entry.getValue();
      LOG.info("dagId: " + dagId + ", finished tasks: " + finishedTasksPerDag);
      StageDag dag = getDagById(dagId);
      for (Integer taskId: finishedTasksPerDag) {
        jobCompleted = emit(spillEventQueue, dagId, taskId) || jobCompleted;
        // move running tasks to finished tasks
        dag.runningTasks.remove(taskId);
        dag.finishedTasks.add(taskId);
      }
    }
    return jobCompleted;
  }

  // return whether the dag has finished
  private boolean emit(Queue<SpillEvent> spillEventQueue, int dagId, int taskId) {
    Map<Integer, Double> data = taskOutputs_.get(taskId);
    taskOutputs_.remove(taskId);
    StageDag dag = getDagById(dagId);
    String stageName = dag.vertexToStage.get(taskId);
    // decrease the num task until 0
    LOG.info("Current dag stage num task map:" + dagStageNumTaskMap_ + ". Now choose dag:" + dagId + ",stage:"+ stageName);
    int numRemaingTasks = dagStageNumTaskMap_.get(dagId).get(stageName) - 1;
    boolean lastSpill = false;
    if (numRemaingTasks == 0) {
      lastSpill = true;
      dagStageNumTaskMap_.get(dagId).remove(stageName);
    } else {
      dagStageNumTaskMap_.get(dagId).put(stageName, numRemaingTasks);
    }
    double timestamp = Simulator.CURRENT_TIME;
    // get parent
    Set<String> parentStages = dag.stages.get(stageName).parents.keySet();
    String parent = "";
    if (!parentStages.isEmpty()) {
      parent = parentStages.iterator().next();  // assume linear graph
    }
    SpillEvent spill = new SpillEvent(data, lastSpill, dagId, stageName, taskId, timestamp, parent);
    boolean endStage = dag.stages.get(stageName).children.isEmpty();
    if (!endStage) {
      LOG.info("new spill event: " + spill);
      spillEventQueue.add(spill);
    }
    boolean jobCompleted = lastSpill && endStage;
    if (jobCompleted) {
      LOG.info("Job completed. DagId = " + dagId + ". Remove it from availablePartitions_");
      availablePartitions_.remove(dagId);
      dag.jobEndTime = timestamp;
      runningJobs_.remove(dag);
      completedJobs_.add(dag);
    }
    return jobCompleted;
  }

  public void nodesFailure(List<Integer> machineIds) {
    numFailureEvents_ += machineIds.size();
    Map<Integer, Set<String>> dagStagesAvailFinal = new HashMap<>();
    for (Integer machineId: machineIds) {
      Map<Integer, Set<String>> dagStagesAvail = nodeFailure(machineId);
      dagStagesAvailFinal.putAll(dagStagesAvail);
    }
    reschedule(dagStagesAvailFinal);
  }

  public Map<Integer, Set<String>> nodeFailure(int machineId) {
    // remove all (dag, stages) that have paritions on that machine
    Map<Integer, Set<String>> dagStagesAvail = new HashMap<>();
    LOG.warning("Machine " + machineId + " fails. All data on that machine is lost. Current time:" + Simulator.CURRENT_TIME);
    for (Map.Entry<Integer, Map<String, Map<Integer, Map<Integer, Partition>>>> entry1: availablePartitions_.entrySet()) {
      int dagId = entry1.getKey();
      Map<String, Map<Integer, Map<Integer, Partition>>> stageMachinePart = entry1.getValue();
      List<String> stageNames = new LinkedList<String>();
      for (Map.Entry<String, Map<Integer, Map<Integer, Partition>>> entry2 : stageMachinePart.entrySet()) {
        String stageName = entry2.getKey();
        if (entry2.getValue().containsKey(machineId)) {
          LOG.info("Dag " + dagId + ", stage " + stageName + " has partitions on machine" + machineId);
          stageNames.add(stageName);
        }
        
      }
      LOG.warning("Dag " + dagId + ", stages: " + stageNames + " are removed");
      stageMachinePart.keySet().removeAll(stageNames);
      // dagStageNumTaskMap_.remove(dagId);  // assume linear
      /* if (dagStageNumTaskMap_.containsKey(dagId)) {
        dagStageNumTaskMap_.get(dagId).keySet().removeAll(stageNames);
      }*/
      dagStagesAvail.put(dagId, new HashSet<>(stageMachinePart.keySet()));
    }
    return dagStagesAvail;
  }


  public void reschedule(Map<Integer, Set<String>> dagStagesAvail) {
    // find current runnable/running stages
    int numKilled = 0;
    for (Map.Entry<Integer, Set<String>> dagStage : dagStagesAvail.entrySet()) {
      int dagId = dagStage.getKey();
      StageDag dag = this.getDagById(dagId);
      Set<Integer> tasksToKill = new HashSet<>();
      // System.out.println("dagId= " + dagId + ", dag= " + dag + ", dagStage= " + dagStage + " tasksTokill=" + tasksToKill);
      String stageToReschedule = dag.onDataLoss(dagStage.getValue(), tasksToKill);
      // TODO: assume only failure once. Use available partition
      if (stageToReschedule != null) {
        // kill tasks
        numKilled += cluster_.killTasks(dagId, tasksToKill);
        if (!dagRunnableStagesMap_.containsKey(dagId)) {
          dagRunnableStagesMap_.put(dagId, new HashSet<>());
        }
        dagRunnableStagesMap_.get(dagId).add(stageToReschedule);
      }
    }
    if (!dagRunnableStagesMap_.isEmpty()) {
      LOG.severe("These (dag, stages) pairs will be reschedule: " + dagRunnableStagesMap_);
    }
    LOG.severe("Reschedule kills " + numKilled + " tasks in total");
  }

  public JSONObject generateStatistics() {
    JSONObject jRoot = new JSONObject();
    JSONArray jJobs = new JSONArray();
    for (BaseDag job: completedJobs_) {
      jJobs.add(((StageDag)job).generateStatistics());
    }
    jRoot.put("jobs", jJobs);
    jRoot.put("num_failures", numFailureEvents_);
    return jRoot;
  }
}


/*
  TODO：
  1 when to check finished tasks? each ready receiving?
  2 need to test whether tasks are really added to the dag. see StageDag::addRunnableTask(Task, int, int, String)

  e.x.
  DAG0: initial data [50, 50, 50, 50, 50, 50, 50, 50, 50, 50]
  Stage0 --- ata --> Stage1 --- o2o --> Stage2
  machnines: M1, M2
  globalpart: 2

  Stage0
  ES
  task0
    - input: (0,25), (1,25), (2,25), (3,25), (4,25), (5,25), (6,25), (7,25), (8,25), (9,25)
    - M1
    - output: (0,25), (1,25), (2,25), (3,25), (4,25), (5,25), (6,25), (7,25), (8,25), (9,25)
  task1:
    - input: (0,25), (1,25), (2,25), (3,25), (4,25), (5,25), (6,25), (7,25), (8,25), (9,25)
    - M1
    - output: (0,25), (1,25), (2,25), (3,25), (4,25), (5,25), (6,25), (7,25), (8,25), (9,25)

  spill event1: (0, 0, 0), (0,25), (1,25), (2,25), (3,25), (4,25), (5,25), (6,25), (7,25), (8,25), (9,25)
  spill event2: (0, 0, 1), (0,25), (1,25), (2,25), (3,25), (4,25), (5,25), (6,25), (7,25), (8,25), (9,25) last
  DS
  Stage0 Partitions (0, 0)
  on spill event1 arrival
M1: Partition[0](:= 0 mod 4) (0, 25), (4, 25), (8, 25)
    Partition[1](:= 1 mod 4) (1, 25), (5, 25), (9, 25)
M2: Partition[2](:= 2 mod 4) (2, 25), (6, 25)
    Partition[2](:= 3 mod 4) (3, 25), (7, 25)
  on spill event2 arrival
M1: Partition[0](:= 0 mod 4) (0, 50), (4, 50), (8, 50)
    Partition[1](:= 1 mod 4) (1, 50), (5, 50), (9, 50)
M2: Partition[2](:= 2 mod 4) (2, 50), (6, 50)
    Partition[2](:= 3 mod 4) (3, 50), (7, 50)


 ready event1 (0, 0) Partition[0] M1
 ready event2 (0, 0) Partition[1] M1
 ready event3 (0, 0) Partition[2] M2
 ready event4 (0, 0) Partition[3] M2 last

  ES
  on ready event1 arrival
  collect (0, 0): incomplete, M1: {P1}
  on ready event2 arrival
  collect (0, 0): incomplete, M1: {P1, P2}
  on ready event3 arrival
  collect (0, 0): incomplete, M1: {P1, P2}, M2: {P3}
  on ready event4 arrival
  collect (0, 0): complete, M1: {P1, P2}, M2: {P3, P4}

  new runnable stages: Stage1
  Stage1
  M1
    task2
      -input: P1, P2
      -output: {(0, 25), (4, 25), (8, 25), (1, 25), (5, 25), (9, 25)}
  M2
    task3
      -input: P3, P4
      -output: {(2, 25), (6, 25), (3, 25), (7, 25)}

  spill event3: (0, 1, 2) {(0, 25), (4, 25), (8, 25), (1, 25), (5, 25), (9, 25)}
  spill event4: (0, 1, 3) {(2, 25), (6, 25), (3, 25), (7, 25)}  last

  DS 
  on spill event3 arrival: 
  Stage 0: ...
  Stage 1: (0, 1)
M1: Partition[0](:= 0 mod 4) (0, 25), (4, 25), (8, 25)
    Partition[1](:= 1 mod 4) (1, 25), (5, 25), (9, 25)

  on spill event4 arrival:
  Stage 0: ...
  Stage 1: (0, 1)
M1: Partition[0](:= 0 mod 4) (0, 25), (4, 25), (8, 25)
    Partition[1](:= 1 mod 4) (1, 25), (5, 25), (9, 25)
M2: Partition[2](:= 2 mod 4) (2, 25), (6, 25)
    Partition[2](:= 3 mod 4) (3, 25), (7, 25)

  ready event5 (0, 1) P1 M1
  ready event6 (0, 1) P2 M1
  ready event7 (0, 1) P3 M2
  ready event8 (0, 1) P4 M2 last

  ES
  on ready event5 arrival
  collect (0, 1): incomplete, M1: {P1}
  on ready event6 arrival
  collect (0, 1): incomplete, M1: {P1, P2}
  on ready event7 arrival
  collect (0, 1): incomplete, M1: {P1, P2}, M2: {P3}
  collect (0, 1): complete, M1: {P1, P2}, M2: {P3, P4}

  new runnable stages: Stage2
  Stage2
  M1
    task4
      -input: P1, P2
  M2
    task5
      -input: P3, P4

  end stage (no spill event)

  failure
T:m0    m1    m0
D:m1    m0    m0
  v0 -- v1 -- v2
  runnning (v1, (t1, t2))
  runnnable (v1, (t3, t4))

  m0 data fails (v1 runnable's parent v0: not available)
  rerun v0

  m1 data fails
  nothing
  
  running (v2, (t1, t2))
  runnable (v2, (t3, t4))
  m0 data fails
  v2 parent v1: not available
  v1 parent v0: available
  rerun v0
 */
