package org.apache.tsfile.bmtool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public enum DataSets { // and its sensors
  TDrive(new ArrayList<>(Arrays.asList("lat", "lon"))),
  GeoLife(new ArrayList<>(Arrays.asList("lat", "lon", "alt"))),
  REDD(new ArrayList<>(Arrays.asList("elec"))),
  TSBS2(new ArrayList<>(Arrays.asList("lat", "lon", "ele", "vel"))),
  TSBS(new ArrayList<>(Arrays.asList("lat", "lon", "ele", "vel")));

  public List<String> sensors;

  DataSets(List<String> ls) {
    sensors = ls;
  }

  public String getConditionBinPath() {
    switch (this) {
      case TDrive:
        return "F:\\0006DataSets\\Conditions\\TDrive-lat-10.bin";
      case GeoLife:
        return "F:\\0006DataSets\\Conditions\\GeoLife-lat-10.bin";
      case REDD:
        return "F:\\0006DataSets\\Conditions\\REDD-elec-10.bin";
      case TSBS:
        return "F:\\0006DataSets\\Conditions\\TSBS-vel-10.bin";
      default:
    }
    return null;
  }

  public String getTargetFile() {
    switch (this) {
      case GeoLife:
        return "TS_FILE_GeoLife_20240315163543_PLAIN_UNCOMPRESSED.tsfile";
      case TDrive:
        return "TS_FILE_TDrive_20240312231153_UNCOMPRESSED.tsfile";
      case REDD:
        return "TS_FILE_REDD_20240312231122_UNCOMPRESSED.tsfile";
      case TSBS:
        return "TS_FILE_TSBS_20240313025222_UNCOMPRESSED.tsfile";
      default:
        return null;
    }
  }

  public String getArrowFile() {
    switch (this) {
      case TDrive:
        return "TDrive.bin";
      case GeoLife:
        return "GeoLife.bin";
      case REDD:
        return "REDD.bin";
      case TSBS:
        return "TSBS.bin";
      case TSBS2:
        return "TSBS2.bin";
      default:
    }
    return null;
  }
}
