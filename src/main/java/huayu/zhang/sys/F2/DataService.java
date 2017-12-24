package huayu.zhang.sys.F2;

import java.util.*;
import java.util.logging.Logger;

import huayu.zhang.sys.datastructures.BaseDag;
import huayu.zhang.sys.utils.Configuration.DataPolicy;


public class DataService {
  private double[] quota_;  // quota per job
  private int numMachines_;
  private int numGlobalPart_;
  private Map<Integer, double[]> usagePerJob_;   // <DagId, list<usage>>
  private Map<Integer, Map<String, StageOutput>> stageOutputPerJob_;  // <DagId, <StageName, intermediate data>>
  private DataPolicy dataPolicy_;   // least quota usage, parent-child separate

  // statistics
  private int statNumSpreadEvents_ = 0;

  private static Logger LOG = Logger.getLogger(DataService.class.getName());

  public DataService(double[] quota, int numGlobalPart, int numMachines, DataPolicy dataPolicy) {
    quota_ = quota;
    numGlobalPart_ = numGlobalPart;
    numMachines_ = numMachines;
    usagePerJob_ = new HashMap<>();
    stageOutputPerJob_ = new HashMap<>();
    dataPolicy_ = dataPolicy;
    LOG.info(String.format("DS created. numMachines: %d, numGlobalPart: %d, data policy:", numMachines_, numGlobalPart_) + dataPolicy);
  }

  public void removeCompletedJobs(Queue<BaseDag> completedJobs) {
    for (BaseDag dag: completedJobs) {
      if (usagePerJob_.containsKey(dag.getDagId())) {
        LOG.fine("Remove completed dag " + dag.getDagId() + " data from DS");
        usagePerJob_.remove(dag.getDagId());
        stageOutputPerJob_.remove(dag.getDagId());
      }
    }
  }

  public void receiveSpillEvents(Queue<SpillEvent> spillEventQueue, Queue<ReadyEvent> readyEventQueue) {
    SpillEvent event = spillEventQueue.poll();
    Map<Integer, Partition> readyParts = null;
    while (event != null) {
      LOG.info(String.format("Receive spill event %d, %s, %d", event.getDagId(), event.getStageName(), event.getTaskId()));
      readyParts = receiveSpillEvent(event);
      for (Map.Entry<Integer, Partition> part: readyParts.entrySet()) {
        readyEventQueue.add(new ReadyEvent(event.getDagId(), event.getStageName(), part.getKey(), part.getValue()));
      }
      event = spillEventQueue.poll();
    }
  }

  public Map<Integer, Partition> receiveSpillEvent(SpillEvent event) {
    int dagId = event.getDagId();
    // int stageId = event.getStageId();
    String stageName = event.getStageName();
    String parentStageName = event.getParentStageName();
    if (!usagePerJob_.containsKey(dagId)) {
      usagePerJob_.put(dagId, new double[numMachines_]);
    }
    if (!stageOutputPerJob_.containsKey(dagId)) {
      stageOutputPerJob_.put(dagId, new HashMap<String, StageOutput>());
    }
    Map<String, StageOutput> dagIntermediateData = stageOutputPerJob_.get(dagId);
    if (!dagIntermediateData.containsKey(stageName)) {
      List<Integer> blackList = new ArrayList<>();
      if (dataPolicy_ == DataPolicy.CPS) {
        if (dagIntermediateData.containsKey(parentStageName)) {
          int[] pMIds = dagIntermediateData.get(parentStageName).getMachineIds();
          for (int i = 0; i < pMIds.length; i++) {
            blackList.add(pMIds[i]);
          }
        }
        LOG.info("Use CPS policy. Blacklist: " + blackList);
      }
      dagIntermediateData.put(stageName, new StageOutput(usagePerJob_.get(dagId), quota_[dagId], numGlobalPart_, blackList));
    }
    //machineId -> usage on that machine
    Map<Integer, Double> newUsage = dagIntermediateData.get(stageName).materialize(event.getData(), event.getTimestamp());
    for (Map.Entry<Integer, Double> entry: newUsage.entrySet()) {
      usagePerJob_.get(dagId)[entry.getKey()] += entry.getValue();
    }

    double[] usage = usagePerJob_.get(dagId);
    int machineChosen = -1;
    Map<String, Set<Integer>> stagePartsToSpread = new HashMap<>();
    Set<Integer> partsToSpreadSet = null;
    Set<Integer> usedMachinesStageSet = null;
    Set<Integer> usedMachinesJobSet = new HashSet<>();
    Comparator<Integer> cmp = new Comparator<Integer>() {
      @Override public int compare(final Integer i, final Integer j) {
        return Double.compare(quota_[dagId] - usage[i], quota_[dagId] - usage[j]);
      }
    };
    for (int i = 0; i < numMachines_; i++) {
      // exceeding quota
      if (usage[i] > quota_[dagId]) {
        String msg = String.format("Dag %d, data usage on machine %d (%f) exceed the quota %f", dagId, i, usage[i], quota_[dagId]);
        LOG.severe(msg);
        throw new RuntimeException(msg);
      }
      if (usage[i] > 0.75 * quota_[dagId]) {
        statNumSpreadEvents_++;
        LOG.warning(String.format("Dag %d use %f storage of the machine %d, which is above 75%% of the quota %f", dagId, usage[i], i, quota_[dagId]));
        // Choose partitions to spread
        for (Map.Entry<String, StageOutput> entry: dagIntermediateData.entrySet()) {
          partsToSpreadSet = entry.getValue().choosePartitionsToSpread(i);
          stagePartsToSpread.put(entry.getKey(), partsToSpreadSet);
          usedMachinesStageSet = entry.getValue().getUsedMachines();
          usedMachinesJobSet.addAll(usedMachinesStageSet);
        }
        // remove machines that already saturated
        for (int j = 0; j < numMachines_; j++) {
          if (usage[j] > 0.75 * quota_[dagId]) {
            usedMachinesStageSet.remove(j);
            usedMachinesJobSet.remove(j);
          }
        }
        // Choose machines to hold the spreaded partitions
        for (Map.Entry<String, Set<Integer>> entry: stagePartsToSpread.entrySet()) {
          stageName = entry.getKey();
          partsToSpreadSet = entry.getValue();
          if (!usedMachinesStageSet.isEmpty()) {
            machineChosen = Collections.max(usedMachinesStageSet, cmp);
          } else if (!usedMachinesJobSet.isEmpty()) {
            machineChosen = Collections.max(usedMachinesJobSet, cmp);
          }
          else {
            for (int j = 0; j < numMachines_; j++) {
              if (usage[j] <= 0.75 * quota_[dagId]) {
                machineChosen = j;
                break;
              }
            }
          }
          for (Integer partitionId : partsToSpreadSet) {
            LOG.warning(String.format("move stage %s, parition %d to machine %d", stageName, partitionId, machineChosen));
            dagIntermediateData.get(stageName).spreadPartition(partitionId, machineChosen);
          }
        }
      }
    }
    // mark complete
    if (event.isLastSpill()) {
       dagIntermediateData.get(stageName).markComplete();
    }
    LOG.fine("Dag " + dagId + " data usage: [" + Arrays.toString(usage) + "], quota:" + quota_[dagId]);
    return dagIntermediateData.get(stageName).getReadyPartitions();
  }

  public void nodesFailure(List<Integer> machineIds) {
    for (Integer machineId: machineIds) {
      nodeFailure(machineId);
    }
  }

  public void nodeFailure(int machineId) {
    // remove all (dag, stages) that have paritions on that machine
    LOG.warning("Machine " + machineId + " fails. All data on that machine is lost.");
    for (Map.Entry<Integer, Map<String, StageOutput>> dagOutput: stageOutputPerJob_.entrySet()) {
      int dagId = dagOutput.getKey();
      Map<String, StageOutput> stageOutputs = dagOutput.getValue();
      List<String> stageNames = new LinkedList<String>();
      // remove related stages
      for (Map.Entry<String, StageOutput> stageKV : stageOutputs.entrySet()) {
        String stageName = stageKV.getKey();
        StageOutput stageOutput = stageKV.getValue();
        if (stageOutput.hasPartitionOnMachine(machineId)) {
          LOG.info("Dag " + dagId + ", stage " + stageName + " has partitions on machine" + machineId);
          stageNames.add(stageName);
          // TODO: reduce data usage
          Map<Integer, Double> msMap = stageOutput.getMachineSizeMap();
          double[] usageJob = usagePerJob_.get(dagId);
          LOG.info("Reduce Dag " + dagId + ", stage " + stageName + " data usage (machine, size)" + msMap);
          for (Map.Entry<Integer, Double> entry: msMap.entrySet()) {
            usageJob[entry.getKey()] -= entry.getValue();
          }
        }
      }
      LOG.warning("Dag " + dagId + ", stages: " + stageNames + " are removed");
      stageOutputs.keySet().removeAll(stageNames);
    }
  }
}
