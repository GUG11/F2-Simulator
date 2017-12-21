package huayu.zhang.sys.simulator;

import huayu.zhang.sys.simulator.Main.Globals.SchedulingPolicy;
import huayu.zhang.sys.simulator.Main.Globals.SharingPolicy;

import java.util.logging.Logger;

public class Main {

  private static Logger LOG = Logger.getLogger(Main.class.getName());
  public static class Globals {

    public static enum SchedulingPolicy { CP };

    public static SchedulingPolicy INTRA_JOB_POLICY = SchedulingPolicy.CP;

    public enum SharingPolicy { Fair, DRF };

    public static SharingPolicy INTER_JOB_POLICY = SharingPolicy.Fair;

    public static int NUM_DIMENSIONS;

    public static double SIM_END_TIME = 20;
    public static double STEP_TIME = .1;

    public static int MAX_NUM_TASKS_DAG = 3000;

    public static boolean TETRIS_UNIVERSAL = false;
    /**
     * these variables control the sensitivity of the simulator to various factors
     * */
    // between 0.0 and 1.0; 0.0 it means jobs are not pessimistic at all

    /**
     * these variables will be set by the static constructor based on runmode
     */
    public static String DataFolder;
    public static String FileOutput;
    public static String pathToInputDagFile = "inputs/dags-input0.json";
    public static String pathToConfig = "inputs/config.json";
    public static String pathToStatsOutput = "logs/stats.json";
  }

  public static void main(String[] args) {

    String UsageStr = "Usage: java huayu.zhang.sys.simulator.Main pathToConfig pathToDags "
        + "resource_dim time_step end_time "
        + "inter_job_policy=[FAIR | DRF] "
        + "intra_job_policy=[CP]";

    // read parameters from command line, if specified
    int curArg = 0;

    if (args.length == curArg) {
      LOG.info(UsageStr + ", curArg=" + curArg);
      System.exit(0);
    }
    Globals.pathToConfig = args[curArg];   // pathToConfig
    curArg++;

    if (args.length == curArg) {
      LOG.info(UsageStr + ", curArg=" + curArg);
      System.exit(0);
    }
    Globals.pathToInputDagFile = args[curArg];   // pathToInput
    curArg++;

    if (args.length == curArg) {
      LOG.info(UsageStr + ", curArg=" + curArg);
      System.exit(0);
    }
    Globals.NUM_DIMENSIONS = Integer.parseInt(args[curArg]); // resource dims
    curArg++;

    if (args.length == curArg) {
      LOG.info(UsageStr + ", curArg=" + curArg);
      System.exit(0);
    }
    Globals.STEP_TIME = Double.parseDouble(args[curArg]); // time step size
    curArg++;

    if (args.length == curArg) {
      LOG.info(UsageStr + ", curArg=" + curArg);
      System.exit(0);
    }
    Globals.SIM_END_TIME = Double.parseDouble(args[curArg]); // time end
    curArg++;

    if (args.length == curArg) {
      LOG.info(UsageStr + ", curArg=" + curArg);
      System.exit(0);
    }
    String UPPER_ARG = args[curArg].toUpperCase();  // inter_job_policy
    curArg++;

    if (UPPER_ARG.contains("FAIR")) {
      Globals.INTER_JOB_POLICY = SharingPolicy.Fair;
    } else if (UPPER_ARG.contains("DRF")) {
      Globals.INTER_JOB_POLICY = SharingPolicy.DRF;
    } else {
      LOG.warning("UNKNOWN INTER_JOB_POLICY");
      System.exit(0);
    }
    if (args.length == curArg) {
      LOG.info(UsageStr + ", curArg=" + curArg);
      LOG.info(UsageStr);
      System.exit(0);
    }

    UPPER_ARG = args[curArg].toUpperCase();  // intra_job_policy
    curArg++;
    if (UPPER_ARG.contains("CP")) {
      Globals.INTRA_JOB_POLICY = SchedulingPolicy.CP;
    } else {
      LOG.warning("UNKNOWN INTRA_JOB_POLICY");
      System.exit(0);
    }

    // sensitivity
    if (args.length != curArg) {
      LOG.info(UsageStr + ", curArg=" + curArg + ", args.length=" + args.length);
      System.exit(0);
    }
    
    
    // print ALL parameters for the record
    System.out.println("=====================");
    System.out.println("Simulation Parameters");
    System.out.println("=====================");
    System.out.println("pathToInputDagFile     = " + Globals.pathToInputDagFile);
    System.out.println("SIMULATION_END_TIME = " + Globals.SIM_END_TIME);
    System.out.println("STEP_TIME           = " + Globals.STEP_TIME);
    System.out.println("NUM_DIMENSIONS      = " + Globals.NUM_DIMENSIONS);
    System.out.println("INTER_JOB_POLICY    = " + Globals.INTER_JOB_POLICY);
    System.out.println("INTRA_JOB_POLICY    = " + Globals.INTRA_JOB_POLICY);
    System.out.println("=====================\n");

    LOG.info("Start simulation ...");
    Simulator simulator = new Simulator();
    simulator.simulate();
    LOG.info("End simulation ...");
  }
}
