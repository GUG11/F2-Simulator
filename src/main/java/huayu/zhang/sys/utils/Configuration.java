package huayu.zhang.sys.utils;

import huayu.zhang.sys.cluster.Cluster;
import huayu.zhang.sys.cluster.Machine;
import huayu.zhang.sys.datastructures.Resources;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import huayu.zhang.sys.simulator.Main.Globals;

import java.io.FileReader;
import java.util.logging.Logger;

public class Configuration {

  public static enum SchedulingPolicy { CP };
  public static enum SharingPolicy { Fair, DRF };
  public static enum DataPolicy { LQU, CPS };


  private JSONParser parser_;
  private JSONObject jCfg_;
  private int numGlobalPart_;
  private int maxPartitionsPerTask_;
  private double endTime_;
  private double timeStep_;

  SchedulingPolicy schePolicy_;
  SharingPolicy sharePolicy_;
  DataPolicy dataPolicy_;

  private static Logger LOG = Logger.getLogger(Configuration.class.getName());

  public Configuration() {
    parser_ = new JSONParser();
  }

  public int getNumGlobalPart() { return numGlobalPart_; }
  public int getMaxPartitionsPerTask() { return maxPartitionsPerTask_; }
  public double getEndTime() { return endTime_; }
  public double getTimeStep() { return timeStep_; }
  public SchedulingPolicy getSchedPolicy() { return schePolicy_; }
  public SharingPolicy getSharePolicy() { return sharePolicy_; }
  public DataPolicy getDataPolicy() { return dataPolicy_; }

  public void parseConfigFile(String filePath) {
    try {
      FileReader fr = new FileReader(filePath);
      jCfg_ = (JSONObject)parser_.parse(fr);
      LOG.info("parse configuration file " + filePath);
      numGlobalPart_ = Integer.parseInt(jCfg_.get("global_partitions_per_machine").toString());
      maxPartitionsPerTask_ = Integer.parseInt(jCfg_.get("max_partitions_in_task").toString());

      timeStep_ = Double.parseDouble(jCfg_.get("time_step").toString());
      endTime_ = Double.parseDouble(jCfg_.get("end_time").toString());

      JSONObject jPolicy = (JSONObject)jCfg_.get("policies");
      parseSchedPolicy(jPolicy.get("intrajob").toString());
      parseSharePolicy(jPolicy.get("interjob").toString());
      parseDataPolicy(jPolicy.get("data").toString());
    } catch (Exception e) {
      System.err.println("Catch exception: " + e);
    }
  }

  public void parseSchedPolicy(String spStr) {
    if (spStr.equals("CP")) {
      schePolicy_ = SchedulingPolicy.CP;
    } else {
      LOG.warning("UNKNOWN INTRA_JOB_POLICY");
      System.exit(0);
    }
  }

  public void parseSharePolicy(String spStr) {
    if (spStr.equals("FAIR")) {
      sharePolicy_ = SharingPolicy.Fair;
    } else if (spStr.equals("DRF")) {
      sharePolicy_ = SharingPolicy.DRF;
    } else {
      LOG.warning("UNKNOWN INTER_JOB_POLICY");
      System.exit(0);
    }
  }

  public void parseDataPolicy(String dpStr) {
    if (dpStr.equals("LQU")) {
      dataPolicy_ = DataPolicy.LQU;
    } else if (dpStr.equals("CPS")) {
      dataPolicy_ = DataPolicy.CPS;
    } else {
      LOG.warning("UNKNOWN INTER_JOB_POLICY");
      System.exit(0);
    }
  }

  public void populateCluster(Cluster cluster) {
    JSONArray jMachines = (JSONArray)jCfg_.get("machines");
    int nextId = 0;
    for (Object jMachine: jMachines) {
      JSONObject jMach = (JSONObject)jMachine;
      double[] res = ((JSONArray)jMach.get("resources")).stream()
                      .mapToDouble(x -> Double.valueOf(x.toString()) )
                      .toArray();
      double failureRate = Double.parseDouble(jMach.get("failure_rate").toString());
      assert Globals.NUM_DIMENSIONS == res.length;
      int replica = Integer.parseInt(jMach.get("replica").toString());
      LOG.info("resource:" + res.length);
      for (int j = 0; j < replica; j++) {
        Machine machine = new Machine(nextId, new Resources(res),
            Double.parseDouble(jMach.get("disk").toString()), failureRate,
            cluster.getExecMode());
        cluster.addMachine(machine);
        nextId++;
      }
    }
  }
}
