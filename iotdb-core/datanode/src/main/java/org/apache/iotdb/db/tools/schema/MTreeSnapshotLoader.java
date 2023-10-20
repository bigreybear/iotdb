/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.tools.schema;

import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaEngineStatistics;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.snapshot.MemMTreeSnapshotUtil;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.loader.MNodeFactoryLoader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

public class MTreeSnapshotLoader {

  static String resultDir = "mtree_data";
  static Map<String, BufferedWriter> pencilCase = new HashMap<>();

  private static SimpleDateFormat tsFormat = new SimpleDateFormat("yyMMddHHmmss");

  private static void close() throws IOException{
    CensusTraveler.queue.clear();
    CensusTraveler.stack.clear();
    for (BufferedWriter writer : pencilCase.values()) {
      writer.close();
    }
  }

  private static BufferedWriter getWriter(String name) throws IOException {
    if (pencilCase.containsKey(name)) {
      return pencilCase.get(name);
    }

    BufferedWriter writer = new BufferedWriter(new FileWriter(
        resultDir + File.separator + name + "_" + tsFormat.format(new Date()) + ".txt")
    );
    pencilCase.put(name, writer);
    return writer;
  }

  private static class DirectoryPathHandler {
    /**
     * With dependency on:
     * org.apache.iotdb.consensus.ratis.SnapshotStorage#TMP_PREFIX
     */
    private static final String TMP_PREFIX = ".tmp.";

    /**
     * With dependency on:
     * org.apache.iotdb.consensus.ratis.SnapshotStorage#getSnapshotDir(String)
     * @return
     */
    private static Path[] getSortedSnapshotDirPaths(File sgStateMachineDir) {
      ArrayList<Path> snapshotPaths = new ArrayList<>();
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(sgStateMachineDir.toPath())) {
        for (Path path : stream) {
          if (path.toFile().isDirectory() && !path.toFile().getName().startsWith(TMP_PREFIX)) {
            snapshotPaths.add(path);
          }
        }
      } catch (IOException exception) {
        exception.printStackTrace(System.out);
        return null;
      }

      Path[] pathArray = snapshotPaths.toArray(new Path[0]);
      Arrays.sort(
          pathArray,
          (o1, o2) -> {
            String index1 = o1.toFile().getName().split("_")[1];
            String index2 = o2.toFile().getName().split("_")[1];
            return Long.compare(Long.parseLong(index1), Long.parseLong(index2));
          });
      return pathArray;
    }

    public static File findLatestSnapshotDir(File sgStateMachineDir) {
      Path[] snapshots = getSortedSnapshotDirPaths(sgStateMachineDir);
      if (snapshots == null || snapshots.length == 0) {
        return null;
      }

      return snapshots[snapshots.length - 1].toFile();
    }
  }

  private static List<IMemMNode> recoverSnapshots(String schemaRegionPath) throws IOException {
    int regionId = 0;

    MemSchemaEngineStatistics engineStatistics = new MemSchemaEngineStatistics();
    MemSchemaRegionStatistics memSchemaRegionStatistics;

    File dir = new File(schemaRegionPath);
    File[] regionDirs = dir.listFiles();
    File latestShot;

    IMemMNode sgNode;
    List<IMemMNode> sgForest = new ArrayList<>();

    for (File regionDir : regionDirs) {
      latestShot = DirectoryPathHandler.findLatestSnapshotDir(regionDir.listFiles((idir, name) -> name.equals("sm"))[0]);
      memSchemaRegionStatistics = new MemSchemaRegionStatistics(regionId++, engineStatistics);
      sgNode = MemMTreeSnapshotUtil.loadSnapshot(
          latestShot,
          measurementMNode -> {},
          deviceMNode -> {},
          memSchemaRegionStatistics
      );
      sgForest.add(sgNode);
    }
    return sgForest;
  }

  public static IMemMNode mergeRegions(List<IMemMNode> regions) {
    IMemMNode root = MNodeFactoryLoader.getInstance().getMemMNodeIMNodeFactory().createAboveDatabaseMNode(null, "root");
    Map<String, IMemMNode> toMerge = new HashMap<>();
    regions.forEach(e -> toMerge.compute(e.getName(), (k, v) -> v == null ? e : recursiveMergeRegions(e, v)));
    toMerge.values().forEach(root::addChild);
    return root;
  }

  private static IMemMNode recursiveMergeRegions(IMemMNode nodeSrc, IMemMNode nodeDst) {
    if (!nodeSrc.getName().equals(nodeDst.getName())) {
      throw new RuntimeException("Invalid merge");
    }

    for (IMemMNode node : nodeSrc.getChildren().values()) {
      if (nodeDst.hasChild(node.getName())) {
        // for conflicted child, merge recursively
        recursiveMergeRegions(node, nodeDst.getChild(node.getName()));
      } else {
        // otherwise add extra node to nodeDst 611017M02
        nodeDst.addChild(node);
      }
    }

    return nodeDst;
  }

  @Deprecated
  private static void checkBaowuDeviceCount(List<IMemMNode> regions) {
    Set<String> devs = new HashSet<>();

    IMemMNode devPar;
    for (IMemMNode node : regions) {
      devPar = node.getChild("baoshan");
      devs.addAll(devPar.getChildren().values().stream().map(IMNode::getName).collect(Collectors.toList()));
      System.out.println(String.format("cnt [%s]: %d, acc: %d", node.getName(), devPar.getChildren().size(), devs.size()));
    }

  }


  public static void main(String[] args) throws IOException {
    IoTDBDescriptor.getInstance().getConfig().setAllocateMemoryForSchemaRegion(Runtime.getRuntime().maxMemory());

    List<IMemMNode> sgForest = recoverSnapshots("mtree_data\\schema_region");

    checkBaowuDeviceCount(sgForest);

    IMemMNode root = mergeRegions(sgForest);

    System.out.println(root);

    CensusTraveler ct = new CensusTraveler(root);

    ct.broadFirst();
  }



  private static class CensusTraveler {
    private IMemMNode root;
    private static Queue<IMemMNode> queue = new LinkedList<>();
    private static Stack<IMemMNode> stack = new Stack<>();

    private int maxLevel;

    // assuming level less than 16
    private int[] maxFanout = new int[16];
    private IMemMNode[] maxFanoutNode = new IMemMNode[16];
    private float[] avgFanout = new float[16];
    private int[] lvlCount = new int[16];

    private int devCnt = 0;
    private int seriesCnt = 0;

    
    CensusTraveler(IMemMNode root) {
      this.root = root;
    }

    private void censusByLevel(int level) {}

    private void updateFanout(int level, int fanout, IMemMNode node) {
      lvlCount[level] ++;
      if (maxFanout[level] < fanout) {
        maxFanout[level] = fanout;
        maxFanoutNode[level] = node;
      }
    }

    private void fanoutResult() {
      for (int i = 0; i < lvlCount.length; i++) {
        if (lvlCount[i] > 0) {
          avgFanout[i] = (float) lvlCount[i+1] / lvlCount[i];
        }
      }
    }

    private void broadFirst() throws IOException {
      int remainCnt = 0; // remaining in this level
      int levelCnt = 1;  // counting on next level
      int level = 0;
      queue.add(root);
      remainCnt++;
      IMemMNode node;

      BufferedWriter seriesListWriter = getWriter("analysis");

      while (queue.size() > 0) {
        node = queue.poll();
        remainCnt--;
        if (remainCnt == 0) {
          level++;
          remainCnt = levelCnt;
          levelCnt = 0;
        }

        if (node.getChildren().size() > 0) {
          queue.addAll(node.getChildren().values());
          levelCnt += node.getChildren().size();
          updateFanout(level, node.getChildren().size(), node);
        } else {
          updateFanout(level, 0, null);
        }

        if (node.isDevice()) {
          devCnt++;
        }

        if (node.isMeasurement()) {
          // seriesListWriter.write(node.getFullPath());
          // seriesListWriter.newLine();
          seriesCnt++;
        }
      }

      maxLevel = level;
      fanoutResult();

      seriesListWriter.write(String.format("Devices: %d, Series: %d, Max Level: %d", devCnt, seriesCnt, maxLevel));
      seriesListWriter.newLine();
      seriesListWriter.write(String.format("Max fanout: %s", Arrays.toString(maxFanout)));
      seriesListWriter.newLine();
      seriesListWriter.write(String.format("Max fanout deviceId: %s", Arrays.stream(maxFanoutNode)
          .filter(Objects::nonNull).map(IMemMNode::getFullPath).collect(Collectors.joining(", "))));
      seriesListWriter.newLine();
      seriesListWriter.write(String.format("Level count: %s", Arrays.toString(lvlCount)));
      seriesListWriter.newLine();
      seriesListWriter.write(String.format("Average fanout: %s", Arrays.toString(avgFanout)));
      seriesListWriter.close();
    }

    private void deepFirst() {}

  }

}
