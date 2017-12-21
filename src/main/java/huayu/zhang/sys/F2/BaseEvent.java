package huayu.zhang.sys.F2;

abstract public class BaseEvent {
  private int dagId_;
  private String stageName_;

  public BaseEvent(int dagId, String stageName) {
    dagId_ = dagId;
    stageName_ = stageName;
  }

  public int getDagId() { return dagId_; }
  public String getStageName() { return stageName_; }
}
