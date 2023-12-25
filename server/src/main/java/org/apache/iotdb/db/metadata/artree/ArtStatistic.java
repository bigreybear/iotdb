package org.apache.iotdb.db.metadata.artree;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ArtStatistic {

  // leaf, Node4, 16, 48, 256
  public int[] nodeCount = new int[5];

  // record how many times a partial key occurs (in different subtree)
  public Map<String, Integer> partialKeyOccur = new HashMap<>();

  // how many leaves a partial key effects
  public Map<String, Integer> partialKeyEffects = new HashMap<>();

  public ArtStatistic() {
  }

  public List<Map.Entry<String, Integer>> mostFrequentPartial() {
    return partialKeyOccur.entrySet().stream()
        .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
        .collect(Collectors.toList());
  }

  public int totalPrefixCompressed() {
    return partialKeyEffects.entrySet().stream().mapToInt(e -> e.getKey().length() * e.getValue()).sum();
  }

}
