package org.apache.iotdb.db.metadata.research;

import io.moquette.persistence.H2Builder;
import org.apache.iotdb.db.metadata.artree.ArtTree;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mtree.MTree;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MTreeAnalyzer {

  public static class LevelStatistic {
    Map<String, Integer> duplicateString = new HashMap<>();
    List<Float> ratioBySubTree = new ArrayList<>();
    int totalNodes = 0, subTrees = 0;
    float totalRatio = 0;
    int level = 0;

    LevelStatistic() {}

    LevelStatistic(int level) {this.level = level;}
  }

  public static void main(String[] args) throws Exception {
    List<String> p = PathTextLoader.getAdjacentPaths(0, 3000, false);
    MTree mTree = new MTree();
    p = PathHandler.filterPathSyntax(p);

    for (String pa : p) {
      mTree.createTimeseries(
          new PartialPath(pa),
          TSDataType.FLOAT,
          TSEncoding.PLAIN,
          CompressionType.UNCOMPRESSED,
          null, null);
    }

    // traverse by level
    List<LevelStatistic> statisticList = new ArrayList<>();
    statisticList.add(new LevelStatistic());
    Deque<IMNode> nodeQueue = new ArrayDeque<>();
    nodeQueue.add(mTree.root);
    int level = 0;
    IMNode node;
    LevelStatistic statistic;
    List<String> levelNames = new ArrayList<>();
    List<String> subTreeNames = new ArrayList<>();
    Deque<IMNode> nextLevelNodes = new ArrayDeque<>();
    while (nodeQueue.size() != 0) {
      node = nodeQueue.poll();

      if (node.isMeasurement()) {
        continue;
      }

      nextLevelNodes.addAll(node.getChildren().values());

      statistic = statisticList.get(level);
      statistic.totalNodes += node.getChildren().size();
      statistic.subTrees += 1;
      statistic.ratioBySubTree.add(
          analyzePrefixRatio(
              node.getChildren()
                  .values()
                  .stream()
                  .map(IMNode::getName)
                  .collect(Collectors.toList()))
      );

      if (nodeQueue.size() == 0 && nextLevelNodes.size() != 0) {
        statistic.totalRatio = analyzeLevelPrefixRatio(
            nextLevelNodes.stream().map(IMNode::getName).collect(Collectors.toList()),
            statistic
        );
        nodeQueue = nextLevelNodes;
        nextLevelNodes = new ArrayDeque<>();
        level++;
        statisticList.add(new LevelStatistic(level));
      }
    }

    System.out.println("SUCCESS");

  }

  public static float analyzeLevelPrefixRatio(List<String> paths, LevelStatistic statistic) throws IOException {
    Set<String> noDupPaths = new HashSet<>();
    for (String path : paths) {
      statistic.duplicateString.merge(path, 1, Integer::sum);
      noDupPaths.add(path);
    }

    return analyzePrefixRatio(noDupPaths.stream().collect(Collectors.toList()));
  }

  public static float analyzePrefixRatio(List<String> paths) throws IOException {
    paths = PathHandler.checkPrefix(paths);
    long totalOri = paths.stream().mapToInt(String::length).sum() + 8L * paths.size();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ArtTree artTree = new ArtTree();
    for (String p : paths) {
      artTree.insert(p.getBytes(), 0L);
    }
    if (artTree.root instanceof org.apache.iotdb.db.metadata.artree.Leaf) {
      return 1.0F;
    }
    artTree.serialize(byteArrayOutputStream);
    if (paths.size() > 500) {
      artTree.collectStatistics();
    }

    return totalOri * 1.0F / byteArrayOutputStream.size();
  }
}
