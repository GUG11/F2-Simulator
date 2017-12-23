package huayu.zhang.sys.utils;

import huayu.zhang.sys.datastructures.BaseDag;
import huayu.zhang.sys.datastructures.Stage;
import huayu.zhang.sys.datastructures.StageDag;
import huayu.zhang.sys.simulator.Main.Globals;

import java.io.*;
import java.util.*;

public class Utils {

  public static double round(double value, int places) {
    double roundedVal = value;
    roundedVal = roundedVal * 100;
    roundedVal = Math.round(roundedVal);
    roundedVal /= 100;
    return roundedVal;
  }
}
