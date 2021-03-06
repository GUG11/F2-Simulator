package huayu.zhang.sys.F2;

import java.util.*;
import java.util.stream.IntStream;
import java.util.logging.Logger;

public class StageOutput {
  private int numMachines_;
  private int[] machineIds_;
  private int numGlobalPart_;
  private int numTotalPartitions_;
  private Map<Integer, Integer> partitionToMachine_;  // for spreading data
  private Set<Integer> readyPartitionSet_;
  private Partition[] partitions_;

  private static Logger LOG = Logger.getLogger(StageOutput.class.getName());

  public StageOutput(int dagId, String stageName, double[] usage, double quota, int numGlobalPart, List<Integer> blackList) {
    setNumMachines(usage, quota);
    assignMachines(usage, blackList);
    numGlobalPart_ = numGlobalPart;
    numTotalPartitions_ = numGlobalPart * numMachines_;
    partitions_ = new Partition[numTotalPartitions_];
    for (int i = 0; i < numTotalPartitions_; i++) {
      partitions_[i] = new Partition(dagId, stageName, i);
    }
    partitionToMachine_ = new HashMap<>();
    readyPartitionSet_ = new HashSet<>();
  }

  public void setNumMachines(double[] usage, double quota) {
    numMachines_ = 0;
    for (int i = 0; i < usage.length; i++) {
      if (usage[i] < 0.75 * quota) numMachines_++;
      if (numMachines_ >= 0.15 * usage.length) break;
    }
    numMachines_ = Math.max(2, numMachines_);
  }

  public void assignMachines(double[] usage, List<Integer> blackList) {
    // TODO: add another data deployment: decolocate current stage and parent stage
    int M = blackList.size(), N = usage.length - M;
    List<Integer> whiteList = new ArrayList<>();
    // devide the machines into whitelist and blacklist
    Set<Integer> blackListSet = new HashSet<>(blackList);
    for (int i = 0; i < usage.length; i++) {
      if (!blackListSet.contains(i)) {
        whiteList.add(i);
      }
    }
    // Sort the list based on quota usage
    Comparator<Integer> cmp = new Comparator<Integer>() {
      @Override public int compare(final Integer i, final Integer j) {
        return Double.compare(usage[i], usage[j]);
      }
    };
    
    Collections.sort(blackList, cmp);
    Collections.sort(whiteList, cmp);
    whiteList.addAll(blackList);
    machineIds_ =  new int[numMachines_];
    for (int i = 0; i < numMachines_; i++) { machineIds_[i] = whiteList.get(i); }
    LOG.info("machineIds_: " + machineIds_);
  }

  public int[] getMachineIds() {
    return machineIds_;
  }

  public Map<Integer, Double> materialize(Map<Integer, Double> data, double time) {
    Map<Integer, Double> machineUsage = new HashMap<>();
    int machineId = -1;
    int partId = -1;
    for (Map.Entry<Integer, Double> entry: data.entrySet()) {
      partId = entry.getKey() % numTotalPartitions_;
      if (partitionToMachine_.containsKey(partId)) {
        machineId = partitionToMachine_.get(partId);
      } else {
        machineId = machineIds_[partId / numGlobalPart_];
      }
      assert (-1 < machineId);
      partitions_[partId].materialize(entry.getKey(), entry.getValue(), machineId, false, time);
      if (!machineUsage.containsKey(machineId)) {
        machineUsage.put(machineId, 0.0);
      }
      machineUsage.put(machineId, Double.valueOf(entry.getValue().doubleValue() + machineUsage.get(machineId).doubleValue()));
    }
    return machineUsage;
  }

  public void markComplete() {
    for (int i = 0; i < partitions_.length; i++) {
      partitions_[i].setComplete();
    }
  }

  public Map<Integer, Partition> getReadyPartitions() {
    Map<Integer, Partition> readyParts = new HashMap<>();
    for (int i = 0; i < partitions_.length; i++) {
      if (partitions_[i].isComplete() && !readyPartitionSet_.contains(i)) { 
        readyParts.put(i, partitions_[i]);
        readyPartitionSet_.add(i);
        if (readyPartitionSet_.size() == numTotalPartitions_) {
          partitions_[i].setLastReady();
        }
      }
    }
    return readyParts;
  }

  public double getAverageSizeOnMachine(int machineId) {
    double result = 0.0;
    for (int i = 0; i < partitions_.length; i++) {
      result += partitions_[i].getPartitionSizeOnMachine(machineId);
    }
    return result / numTotalPartitions_;
  }

  public double getAverageIncreaseRate(int machineId) {
    double result = 0.0;
    for (int i = 0; i < partitions_.length; i++) {
      result += partitions_[i].getIncreaseRate(machineId);
    }
    return result / numTotalPartitions_;
  }

  public Set<Integer> choosePartitionsToSpread(int machineId) {
    double threshold = 0.25;
    Set<Integer> partsToSpreadSet = new HashSet<>();
    double avgSize = this.getAverageSizeOnMachine(machineId);
    double avgIncr = this.getAverageIncreaseRate(machineId);
    for (int i = 0; i < partitions_.length; i++) {
      if (partitions_[i].getPartitionSizeOnMachine(machineId) > threshold * avgSize ||
          partitions_[i].getIncreaseRate(machineId) > threshold * avgIncr) {
        partsToSpreadSet.add(i); 
      }
    }
    return partsToSpreadSet;
  }

  public Set<Integer> getUsedMachines() {
    Set<Integer> machineSet = new HashSet<Integer>(Arrays.asList(Arrays.stream(machineIds_).boxed().toArray(Integer[]::new)));
    for (Map.Entry<Integer, Integer> entry: partitionToMachine_.entrySet()) {
      machineSet.add(entry.getValue());
    }
    return machineSet;
  }
  
  public void spreadPartition(int partitionId, int machineId) {
    partitionToMachine_.put(partitionId, machineId);
  }

  public boolean hasPartitionOnMachine(int machineId) {
    for (int i = 0; i < partitions_.length; i++) {
      if (partitions_[i].isOnMachine(machineId)) {
        return true;
      }
    }
    return false;
  }

  public Map<Integer, Double> getMachineSizeMap() {
    Map<Integer, Double> machineSizeMap = new HashMap<>();
    Partition partition = null;
    for (int i = 0; i < partitions_.length; i++) {
      Map<Integer, Double> msMapPart = partitions_[i].getMachineSizeMap();
      for (Map.Entry<Integer, Double> entry: msMapPart.entrySet()) {
        int mid = entry.getKey();
        if (!machineSizeMap.containsKey(mid)) {
          machineSizeMap.put(mid, 0.0);
        }
        double newSize = entry.getValue() + machineSizeMap.get(mid);
        machineSizeMap.put(mid, newSize);
      }
    }
    return machineSizeMap;
  }
}
