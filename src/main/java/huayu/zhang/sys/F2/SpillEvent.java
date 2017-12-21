package huayu.zhang.sys.F2;

import java.util.Map;

public class SpillEvent extends BaseEvent {
  Map<Integer, Double> data_; // size of intermediate data of each key
  private String parentStageName_;    // assume linear
  private boolean lastSpill_;
  private int taskId_;
  private double timestamp_;

  public SpillEvent(Map<Integer, Double> data, boolean lastSpill,
      int dagId, String stageName, int taskId, double timestamp, String parentStageName) {
    super(dagId, stageName);
    parentStageName_ = parentStageName;
    data_ = data;
    lastSpill_ = lastSpill;
    taskId_ = taskId;
    timestamp_ = timestamp;
  }

  public Map<Integer, Double> getData() { return data_; }
  public boolean isLastSpill() { return lastSpill_; }
  public int getTaskId() { return taskId_; }
  public double getTimestamp() { return timestamp_; }
  public String getParentStageName() { return parentStageName_; }
  public void setTimestamp(double timestamp) { timestamp_ = timestamp; }

  @Override
  public String toString() {
    return String.format("dagId: %d, stageName: %s, parentStage: %s, taskId: %d", this.getDagId(), this.getStageName(), parentStageName_, taskId_) + ", data: " + data_ + ", lastSpill: " + lastSpill_; 
  }
}
