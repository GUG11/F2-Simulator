package huayu.zhang.sys.utils;

import huayu.zhang.sys.datastructures.BaseDag;
import huayu.zhang.sys.datastructures.Stage;
import huayu.zhang.sys.datastructures.StageDag;
import huayu.zhang.sys.simulator.Main.Globals;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class DagParser {
  private JSONParser parser_;

  public DagParser() {
    parser_ = new JSONParser();
  }
  
  public Queue<BaseDag> parseDAGSpecFile(String filePath) {
    Queue<BaseDag> dags = new LinkedList<BaseDag>();
    try {
      FileReader fr = new FileReader(filePath);
      JSONArray jDags = (JSONArray)parser_.parse(fr);

      for (Object jDag: jDags) {
        dags.add(parseDAG((JSONObject)jDag));
      }
    } catch (Exception e) {
      System.err.println("Catch exception: " + e);
    }
    return dags;
  }

  public StageDag parseDAG(JSONObject jDag) {
    double[] keySizes = ((JSONArray)jDag.get("key_sizes")).stream().mapToDouble(x -> Double.valueOf(x.toString()) ).toArray();
    StageDag dag = new StageDag(Integer.parseInt(jDag.get("dagID").toString()), 
        Double.parseDouble(jDag.get("quota").toString()),
        keySizes,
        Double.parseDouble(jDag.get("arrival_time").toString()));

    JSONArray jStages = (JSONArray)jDag.get("stages");
    for (int i = 0; i < jStages.size(); i++) {
      JSONObject jStage = (JSONObject)jStages.get(i);
      dag.addStage(jStage.get("name").toString(), parseStage(jStage));
    }
    JSONArray jDeps = (JSONArray)jDag.get("dependencies");
    for (Object jDep: jDeps) {
      parseDependency((JSONObject)jDep, dag);
    }

    dag.setCriticalPaths();
    return dag;
  }

  public Stage parseStage(JSONObject jStage) {
    int numTask = Integer.parseInt(jStage.get("num_tasks").toString());
    double outinRatio = Double.parseDouble(jStage.get("outin_ratio").toString());
    double[] resc = ((JSONArray)jStage.get("resources")).stream().mapToDouble(x -> Double.valueOf(x.toString()) ).toArray();
    assert Globals.NUM_DIMENSIONS == resc.length;
    return new Stage(jStage.get("name").toString(), numTask,
          Double.parseDouble(jStage.get("duration").toString()),
          resc, outinRatio);
  }

  public void parseDependency(JSONObject jDep, StageDag dag) {
    dag.populateParentsAndChildrenStructure(jDep.get("src").toString(),
        jDep.get("dst").toString());
  }

  public double[] parseInputData(String filePath) {
    double[] keySizes = null;
    try {
      FileReader fr = new FileReader(filePath);
      JSONObject jData = (JSONObject)parser_.parse(fr);
      keySizes = ((JSONArray)jData.get("key_sizes")).stream().mapToDouble(x -> Double.valueOf(x.toString()) ).toArray();
    } catch (Exception e) {
      System.err.println("Catch exception: " + e);
    }
    return keySizes;
  }
}
