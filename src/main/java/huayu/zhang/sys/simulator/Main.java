package huayu.zhang.sys.simulator;

import java.util.logging.Logger;

public class Main {

  private static Logger LOG = Logger.getLogger(Main.class.getName());
  public static class Globals {

    public static int NUM_DIMENSIONS;

    public static int MAX_NUM_TASKS_DAG = 3000;
  }

  public static void main(String[] args) {
    String pathToInputDagFile;
    String pathToConfig;
    String pathToStatsOutput = "logs/stats.json";

    String UsageStr = "Usage: java huayu.zhang.sys.simulator.Main pathToConfig pathToDags "
        + "resource_dim time_step end_time ";

    // read parameters from command line, if specified
    int curArg = 0;

    if (args.length == curArg) {
      LOG.info(UsageStr + ", curArg=" + curArg);
      System.exit(0);
    }
    pathToConfig = args[curArg];   // pathToConfig
    curArg++;

    if (args.length == curArg) {
      LOG.info(UsageStr + ", curArg=" + curArg);
      System.exit(0);
    }
    pathToInputDagFile = args[curArg];   // pathToInput
    curArg++;

    if (args.length == curArg) {
      LOG.info(UsageStr + ", curArg=" + curArg);
      System.exit(0);
    }
    Globals.NUM_DIMENSIONS = Integer.parseInt(args[curArg]); // resource dims
    curArg++;

    // sensitivity
    if (args.length != curArg) {
      LOG.info(UsageStr + ", curArg=" + curArg + ", args.length=" + args.length);
      System.exit(0);
    }
    
    
    // print ALL parameters for the record
    System.out.println("=====================");
    System.out.println("Simulation Parameters");
    System.out.println("=====================");
    System.out.println("pathToInputDagFile     = " + pathToInputDagFile);
    System.out.println("pathToConfigFile     = " + pathToConfig);
    System.out.println("NUM_DIMENSIONS      = " + Globals.NUM_DIMENSIONS);
    System.out.println("=====================\n");

    LOG.info("Start simulation ...");
    Simulator simulator = new Simulator(pathToInputDagFile, pathToConfig, pathToStatsOutput);
    simulator.simulate();
    LOG.info("End simulation ...");
  }
}
