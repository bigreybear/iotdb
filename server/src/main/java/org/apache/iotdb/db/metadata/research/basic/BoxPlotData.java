package org.apache.iotdb.db.metadata.research.basic;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BoxPlotData {

  public static Map<String, Float> calculateBoxPlotData(List<Float> data) {
    if (data.size() == 0) {
      return null;
    }

    Collections.sort(data);

    Map<String, Float> boxPlotData = new HashMap<>();
    boxPlotData.put("Min", data.get(0));
    boxPlotData.put("Max", data.get(data.size() - 1));
    boxPlotData.put("Median", calculateMedian(data));
    boxPlotData.put("Q1", calculatePercentile(data, 25));
    boxPlotData.put("Q3", calculatePercentile(data, 75));

    return boxPlotData;
  }

  private static float calculateMedian(List<Float> data) {
    int size = data.size();
    if (size % 2 == 0) {
      return (data.get(size / 2 - 1) + data.get(size / 2)) / 2;
    } else {
      return data.get(size / 2);
    }
  }

  private static float calculatePercentile(List<Float> data, double percentile) {
    int index = (int) Math.ceil(percentile / 100.0 * data.size()) - 1;
    return data.get(index);
  }
}
