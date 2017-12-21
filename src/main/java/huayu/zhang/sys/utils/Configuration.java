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
  private JSONParser parser_;
  private JSONObject jCfg_;
  private int numGlobalPart_;
  private int maxPartitionsPerTask_;
  private String strategy_;

  private static Logger LOG = Logger.getLogger(Configuration.class.getName());

  public Configuration() {
    parser_ = new JSONParser();
  }

  public int getNumGlobalPart() { return numGlobalPart_; }
  public int getMaxPartitionsPerTask() { return maxPartitionsPerTask_; }
  public String getStrategy() { return strategy_; }

  public void parseConfigFile(String filePath) {
    try {
      FileReader fr = new FileReader(filePath);
      jCfg_ = (JSONObject)parser_.parse(fr);
      LOG.info("parse configuration file " + filePath);
      numGlobalPart_ = Integer.parseInt(jCfg_.get("global_partitions_per_machine").toString());
      maxPartitionsPerTask_ = Integer.parseInt(jCfg_.get("max_partitions_in_task").toString());
      strategy_ = jCfg_.get("strategy").toString();
    } catch (Exception e) {
      System.err.println("Catch exception: " + e);
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
