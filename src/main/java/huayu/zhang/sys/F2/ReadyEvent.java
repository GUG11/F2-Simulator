package huayu.zhang.sys.F2;

public class ReadyEvent extends BaseEvent {
  private int partitionId_;
  private Partition partition_;

  public ReadyEvent(int dagId, String name, int partitionId, Partition partition) {
    super(dagId, name);
    partitionId_ = partitionId;
    partition_ = partition;
  }

  public int getPartitionId() { return partitionId_; }
  public Partition getPartition() { return partition_; }
  public boolean isLastPartition() { return partition_.isLastPartReady(); }

  @Override
  public String toString() {
    return String.format("dagId: %d, stageName: %s, partitionId: %d", getDagId(), getStageName(), partitionId_) + ", lastReady=" + isLastPartition() + " content: " + partition_;
  }
}
