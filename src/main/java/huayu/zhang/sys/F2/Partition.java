package huayu.zhang.sys.F2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Partition {
  private int dagId_;
  private String stageName_;
  private int partId_;
  private Map<Integer, Map<Integer, Double>> machineKeySize_;    // <machineId, (key, size)>
  private double totalSize_;
  private double increaseRate_;
  private boolean complete_;
  private boolean lastPartReady_;
  private double lastUpdateTime_;
  private static double TIMETOL=0.001;
  private int activeMachineId_;
  private List<Integer> machinesInvolved_;
  public Partition(int dagId, String stageName, int partId) {
    dagId_ = dagId;
    stageName_ = stageName;
    partId_ = partId;
    machineKeySize_ = new HashMap<Integer, Map<Integer, Double>>();
    totalSize_ = 0;
    complete_ = false;
    lastPartReady_ = false;
    lastUpdateTime_ = -100.0;
    increaseRate_ = 0.0;
    activeMachineId_ = -1;
    machinesInvolved_ = new ArrayList<>();
  }

  public int getDagId() { return dagId_; }
  public int getPartId() { return partId_; }
  public String getStageName() { return stageName_; }

  public void materialize(Integer key, double size, int machineId, boolean lastData, double time) {
    activeMachineId_ = machineId;
    if (!machineKeySize_.containsKey(machineId)) {
      machineKeySize_.put(machineId, new HashMap<Integer, Double>());
      machinesInvolved_.add(machineId);
    }
    Map<Integer, Double> keySize_ = machineKeySize_.get(machineId);
    if (!keySize_.containsKey(key)) {
      keySize_.put(key, 0.0);
    }
    keySize_.put(key, Double.valueOf(size + keySize_.get(key).doubleValue()));
    if (lastUpdateTime_ + TIMETOL < time) {
      lastUpdateTime_ = time;
      increaseRate_ = size;
    } else {
      increaseRate_ += size;
    }
    totalSize_ += size;
    complete_ = lastData;
  }

  public Map<Integer, Double> getMachineSizeMap() {
    Map<Integer, Double> machineSizeMap = new HashMap<>();
    for (Map.Entry<Integer, Map<Integer, Double>> entry : machineKeySize_.entrySet()) {
      int machineId = entry.getKey();
      double dataSize = entry.getValue().values().stream().mapToDouble(v -> v.doubleValue()).sum();
      machineSizeMap.put(machineId, dataSize);
    }
    return machineSizeMap;
  }

  public List<Integer> getMachinesInvolved() {return machinesInvolved_; }
  public boolean isLastPartReady() { return lastPartReady_; }
  public boolean isComplete() { return complete_; }
  public void setComplete() { complete_ = true; }
  public void setLastReady() { lastPartReady_ = true; }
  public double getIncreaseRate(int machineId) { 
    return machineId == activeMachineId_ ? increaseRate_ : 0.0;
  }
  public Map<Integer, Map<Integer, Double>> getData() { return machineKeySize_; }

  public double getPartitionSizeOnMachine(int machineId) {
    if (machineKeySize_.containsKey(machineId)) {
      return machineKeySize_.get(machineId).values().stream().mapToDouble(v -> v).sum();
    } else {
      return 0.0;
    }
  }
  public void aggregateKeyShareToSingleMachine(int id, List<Integer> machines) {
    assert (-1 < id);
    Map<Integer, Double> dest = this.machineKeySize_.get(id);
    for(Integer i : machines) {
      Map<Integer, Double> keyShare = this.machineKeySize_.get(i);
      for(Map.Entry<Integer, Double> entry: keyShare.entrySet()) {
        int key = entry.getKey();
        double cur = entry.getValue() + dest.get(key);
        dest.put(key, cur);
      }
    }
    this.machineKeySize_.clear();
    this.machineKeySize_.put(id, dest);

    this.machinesInvolved_.clear();
    this.machinesInvolved_.add(id);

  }

  @Override
  public String toString() {
    return String.format("(Dag:%d, Stage:%s, Partition:%d): %s",
        this.dagId_, this.stageName_, this.partId_, machineKeySize_.toString());
  }

  @Override
  public int hashCode() { return partId_; }

  @Override
  public boolean equals(Object obj) {
    Partition pObj = (Partition)obj;
    return this.dagId_ == pObj.getDagId() && 
      this.stageName_ == pObj.getStageName() &&
      this.partId_ == pObj.getPartId();
  }

  public boolean isOnMachine(int machineId) {
    return machineKeySize_.containsKey(machineId);
  }
}
