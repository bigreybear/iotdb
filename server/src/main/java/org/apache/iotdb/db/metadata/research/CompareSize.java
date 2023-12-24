package org.apache.iotdb.db.metadata.research;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.artree.ArtTree;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.metadata.research.PathHandler.*;

public class CompareSize {
  public static final int DATASET_SIZE = 400000;

  private static final String FILE_PATH = "mtree_test".concat("_TsFileIOWriterTest.tsfile");
  private static final int[] SIZE_TEST_SCALE = {1000, 2000, 3000, 5000};
  private static int LARGEST_SCALE = SIZE_TEST_SCALE[SIZE_TEST_SCALE.length - 1];
  private static final int TEST_RUN = 3; // for guarantee stability

  private static final Map<String, List<int[]>> dimsToResults = new HashMap<>();

  private static final Logger logger = LoggerFactory.getLogger(CompareSize.class);

  public static void main(String[] args) throws IOException, IllegalPathException {

    List<String> p;
    for (int i = 0; i < TEST_RUN; i++) {
      System.out.println("Test Run:" + i);

//      p = PathTextLoader.getAdjacentPaths(LARGEST_SCALE, true);
//      standardCompareSize(p, "Adjacent With Chinese");
//
//      p = PathTextLoader.getAdjacentPaths(LARGEST_SCALE, false);
//      standardCompareSize(p, "Adjacent Without Chinese");
//
//      p = PathTextLoader.getRandomPaths(LARGEST_SCALE, true);
//      standardCompareSize(p, "Random With Chinese");
//
//      p = PathTextLoader.getRandomPaths(LARGEST_SCALE, false);
//      standardCompareSize(p, "Random Without Chinese");

      p = PathTextLoader.getAdjacentPaths(LARGEST_SCALE, false);
      System.out.println(analyzeDevices(p));
      p = adjustWithBaoWuModeling(p);
      System.out.println(analyzeDevices(p));
//      standardCompareSize(p, "Adjacent Without Chinese, Adjusted");

//      p = PathTextLoader.getAdjacentPaths(LARGEST_SCALE, true);
//      p = adjustWithBaoWuModeling(p);
//      standardCompareSize(p, "Adjacent With Chinese, Adjusted");
//
//      p = PathTextLoader.getRandomPaths(LARGEST_SCALE, false);
//      p = adjustWithBaoWuModeling(p);
//      standardCompareSize(p, "Random Without Chinese, Adjusted");
//
//      p = PathTextLoader.getRandomPaths(LARGEST_SCALE, true);
//      p = adjustWithBaoWuModeling(p);
//      standardCompareSize(p, "Random With Chinese, Adjusted");
    }
    normalizeAndPrintResults();

//    compareWithFileSize(false);
    // compareWithFileSize(true);
    // int totalLength;
    // int testSize = 2000;
    //
    // Random random = new Random(System.currentTimeMillis());
    // int randomStart = random.nextInt(600000);
    //
    // System.out.println("For adjacent paths.");
    // List<String> paths = PathTextLoader.getAdjacentPathsWithoutChinese(randomStart, testSize);
    // totalLength = paths.stream().mapToInt(String::length).sum();
    // System.out.printf("Total length of original paths:%d%n", totalLength);
    // testARTFileSize(paths);
    // long tsfile = testTsFile(paths);
    // System.out.println(String.format("Cost %d bytes with %d paths with TsFile", tsfile,
    // paths.size()));
    //
    //
    // System.out.println("");
    // System.out.println("For random paths.");
    // paths = PathTextLoader.getRandomPathsWithoutChinese(testSize);
    // totalLength = paths.stream().mapToInt(String::length).sum();
    // System.out.printf("Total length of original paths:%d%n", totalLength);
    // testARTFileSize(paths);
    // tsfile = testTsFile(paths);
    // System.out.println(String.format("Cost %d bytes with %d paths with TsFile", tsfile,
    // paths.size()));
  }

  /**
   * Dimensions to control comparison:<br>
   *  1. w/o Chinese<br>
   *  2. adjust modeling (.Value suffix for BaoWu case)<br>
   *  3. random or adajcent paths<br>
   *
   * <p>Configs to comparison:<br>
   *  1. scale<br>
   *  2. run numbers<br>
   *
   * <p>Should always align with TsMeta otherwise unfair.<br>
   *
   * Thus standard process as:<br>
   *  1. prepare paths with dimensions.(Chinese?adjust?adjacent?)<br>
   *  1.1 Prefix check necessary<br>
   *  2. ingest TsFile<br>
   *  3. align with TsMeta<br>
   *  4. ingest Art<br>
   *  5. result<br>
   */
  public static void standardCompareSize(List<String> naivePaths, String dims) throws IllegalPathException, IOException {
    if (naivePaths.size() < SIZE_TEST_SCALE[SIZE_TEST_SCALE.length - 1]) {
      logger.error("Paths not enough to perform the largest scale test.");
      return;
    }

    // ori for length of aligned paths
    int[] ori, art, tsf;

    if (dimsToResults.containsKey(dims)) {
      List<int[]> r = dimsToResults.get(dims);
      ori = r.get(0);
      art = r.get(1);
      tsf = r.get(2);
    } else {
      ori = new int[SIZE_TEST_SCALE.length];
      art = new int[SIZE_TEST_SCALE.length];
      tsf = new int[SIZE_TEST_SCALE.length];
      dimsToResults.put(dims, new ArrayList<>(Arrays.asList(ori, art, tsf)));
    }

    List<String> paths;
    for (int i = 0; i < SIZE_TEST_SCALE.length; i++) {
      paths = naivePaths.subList(0, SIZE_TEST_SCALE[i]);
      paths = filterPathSyntax(paths);
      paths = checkPrefix(paths);
      tsf[i] += testTsFile(paths);
      paths = alignPathsWithTsMeta(paths);
      ori[i] = paths.stream().mapToInt(String::length).sum();
      art[i] += testARTFileSize(paths);
    }
  }

  public static void normalizeAndPrintResults() {
    for (Map.Entry<String, List<int[]>> e : dimsToResults.entrySet()){
      for (int[] a : e.getValue()) {
        for (int i = 0; i < a.length; i++) {
          a[i] = a[i] / TEST_RUN;
        }
      }

      StringBuilder builder = new StringBuilder();
      builder.append(String.format("%s:\n", e.getKey()));
      builder.append(String.format("ori: %s:\n", printArrayAsOneLine(e.getValue().get(0))));
      builder.append(String.format("art: %s:\n", printArrayAsOneLine(e.getValue().get(1))));
      builder.append(String.format("tsf: %s:\n", printArrayAsOneLine(e.getValue().get(2))));
      System.out.println(builder);
    }
  }

  // NOTE logic:
  // decide size, choose paths, check syntax, calc ori, art, tsfile, change size
  @Deprecated
  public static void compareWithFileSize(boolean random) throws IOException, IllegalPathException {
    String title = random ? "Random selected Paths." : "Adjacent Selected Paths.";
    // original paths
    int[] ori = new int[SIZE_TEST_SCALE.length];
    int[] art = new int[SIZE_TEST_SCALE.length];
    int[] tsf = new int[SIZE_TEST_SCALE.length];

    // paths aligned
    int[] oriB = new int[SIZE_TEST_SCALE.length];
    int[] artB = new int[SIZE_TEST_SCALE.length];
    int[] tsfB = new int[SIZE_TEST_SCALE.length];

    // paths modeling restored and aligned (removed impact from user
    int[] oriC = new int[SIZE_TEST_SCALE.length];
    int[] artC = new int[SIZE_TEST_SCALE.length];
    int[] tsfC = new int[SIZE_TEST_SCALE.length];

    Random random1 = new Random(System.currentTimeMillis());
    int startPoint = random1.nextInt(DATASET_SIZE);

    List<String> paths = null, cursor = null, cursor2 = null;
    for (int j = 0; j < TEST_RUN; j++) {
      logger.info("Start at run:{}", j);
      for (int i = 0; i < SIZE_TEST_SCALE.length; i++) {
        paths =
            random
                ? PathTextLoader.getRandomPathsWithoutChinese(SIZE_TEST_SCALE[i])
                : PathTextLoader.getAdjacentPathsWithoutChinese(12345, SIZE_TEST_SCALE[i]);

        paths = filterPathSyntax(paths);
        // ori[i] += paths.stream().mapToInt(String::length).sum();
        // art[i] += testARTFileSize(paths);
        // tsf[i] += testTsFile(paths);

        cursor = adjustWithBaoWuModeling(paths);
        cursor = checkPrefix(cursor);
        oriB[i] += cursor.stream().mapToInt(String::length).sum();
        artB[i] += testARTFileSize(cursor);
        tsfB[i] += testTsFile(cursor);

        cursor2 = adjustWithBaoWuModeling(paths);
        System.out.println(analyzeDevices(cursor2));
        cursor2 = alignPathsWithTsMeta(cursor2);
        cursor2 = checkPrefix(cursor2);

        // 如果有设备超过 256 序列，这种 align 是有误差的：外挂对齐后，实际 tsfile 只关心对其后的第 1+256n 路径
        // 例如，设备有 280 序列，预先对齐后仅写入 2 条，此时 tsfile 再抽取后仅在 MIN 存储 1 序列；
        // 如果不作预对齐，MIN 要存 2 序列。因此有差距但不大。NOTE

        oriC[i] += cursor2.stream().mapToInt(String::length).sum();
        artC[i] += testARTFileSize(cursor2);
        tsfC[i] += testTsFile(cursor2);

        logger.info("Finish at scale:{}", SIZE_TEST_SCALE[i]);
      }
    }

    ori = Arrays.stream(ori).map(x -> x / TEST_RUN).toArray();
    art = Arrays.stream(art).map(x -> x / TEST_RUN).toArray();
    tsf = Arrays.stream(tsf).map(x -> x / TEST_RUN).toArray();

    oriB = Arrays.stream(oriB).map(x -> x / TEST_RUN).toArray();
    artB = Arrays.stream(artB).map(x -> x / TEST_RUN).toArray();
    tsfB = Arrays.stream(tsfB).map(x -> x / TEST_RUN).toArray();

    oriC = Arrays.stream(oriC).map(x -> x / TEST_RUN).toArray();
    artC = Arrays.stream(artC).map(x -> x / TEST_RUN).toArray();
    tsfC = Arrays.stream(tsfC).map(x -> x / TEST_RUN).toArray();

    System.out.println(String.format("%s(%s)", title, analyzeDevices(paths)));
    System.out.printf("path:  %s%n", printArrayAsOneLine(SIZE_TEST_SCALE));
    System.out.printf("path:  %s%n", printArrayAsOneLine(ori));
    System.out.printf("art:   %s%n", printArrayAsOneLine(art));
    System.out.printf("tsfile:%s%n", printArrayAsOneLine(tsf));
    System.out.println(
        String.format(
            "Adjust modeling method but not align TsFile Meta design:(%s)",
            analyzeDevices(cursor)));
    System.out.printf("path:  %s%n", printArrayAsOneLine(oriB));
    System.out.printf("art:   %s%n", printArrayAsOneLine(artB));
    System.out.printf("tsfile:%s%n", printArrayAsOneLine(tsfB));
    System.out.println(
        String.format(
            "Adjust modeling method and align with TsFile Meta Design:(%s)",
            analyzeDevices(cursor2)));
    System.out.printf("path:  %s%n", printArrayAsOneLine(oriC));
    System.out.printf("art:   %s%n", printArrayAsOneLine(artC));
    System.out.printf("tsfile:%s%n", printArrayAsOneLine(tsfC));
  }

  // check device status right before run test
  public static String analyzeDevices(List<String> paths) {
    // device -> measurements
    Map<String, TreeSet<String>> m1 = new TreeMap<>();
    PartialPath pp;
    int pathTotalLen = 0, sensorTotalLen = 0, validPaths = 0;
    for (String p : paths) {
      try {
        pp = new PartialPath(p);
        validPaths ++;
        pathTotalLen += p.length();
        sensorTotalLen += pp.getMeasurement().length();
        m1.computeIfAbsent(pp.getDevice(), k -> new TreeSet<>()).add(pp.getMeasurement());
      } catch (Exception e) {
      }
    }

    return String.format(
        "Input paths:%d, Valid paths:%d, devices:%d, Ave path len:%d, Ave Sensor Len:%d",
        paths.size(), validPaths, m1.size(), pathTotalLen/validPaths, sensorTotalLen/validPaths);
  }

  // region Compare Body

  public static int testARTFileSize(List<String> paths) throws IOException {
    ArtTree tree = new ArtTree();
    Map<String, Long> answerCheck = new HashMap<>();
    for (int i = 0; i < paths.size(); i++) {
      tree.insert(paths.get(i).getBytes(), (long) i);
      // answerCheck.put(paths.get(i), (long) i);
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    return tree.serialize(baos);

    // ArtTree.calculateDepth(tree);
    // serialize(((ArtNode)tree.root), baos);
    // ReadWriteIOUtils.write(tree.root.offset, baos);
    // return baos.size();
    // System.out.println(String.format("Cost %d bytes with %d paths with ART", baos.size(),
    // paths.size()));
    // byte[] res = baos.toByteArray();
    //    traverse((ArtNode) tree.root, "");
    //    ArtNode r2 = (ArtNode) deserialize(ByteBuffer.wrap(res));
    //    traverseAfterDeserialize(r2, "");
  }

  public static int testTsFile(List<String> paths) throws IOException, IllegalPathException {
    Map<String, Set<MeasurementSchema>> devAndSensirs = new HashMap<>();

    // parse paths
    PartialPath pp;
    for (String path : paths) {
      pp = new PartialPath(path);
      if (!devAndSensirs.containsKey(pp.getDevice())) {
        devAndSensirs.put(pp.getDevice(), new HashSet<>());
      }
      devAndSensirs
          .get(pp.getDevice())
          .add(new MeasurementSchema(pp.getMeasurement(), TSDataType.INT64, TSEncoding.RLE));
    }

    File f1 = new File(FILE_PATH);
    f1.deleteOnExit();
    TsFileIOWriter writer = new TsFileIOWriter(new File(FILE_PATH));

    for (Map.Entry<String, Set<MeasurementSchema>> entry : devAndSensirs.entrySet()) {
      writeChunkGroup2(writer, entry.getKey(), entry.getValue());
    }

    writer.setMinPlanIndex(100);
    writer.setMaxPlanIndex(10000);
    writer.writePlanIndices();
    // end file
    writer.endFile();

    return (int) writer.metadataCost;
  }

  // endregion

  public static void writeChunkGroup2(
      TsFileIOWriter writer, String dev, Set<MeasurementSchema> schemas) throws IOException {
    for (MeasurementSchema measurementSchema : schemas) {
      // chunk group
      writer.startChunkGroup(dev);
      // ordinary chunk, chunk statistics
      Statistics statistics = Statistics.getStatsByType(measurementSchema.getType());
      statistics.updateStats(0L, 0L);
      writer.startFlushChunk(
          measurementSchema.getMeasurementId(),
          measurementSchema.getCompressor(),
          measurementSchema.getType(),
          measurementSchema.getEncodingType(),
          statistics,
          0,
          0,
          0);
      writer.endCurrentChunk();
      writer.endChunkGroup();
    }
  }

  // region Utils
  public static String checkList(List<String> l1, List<String> l2) {
    if (l1.size() != l2.size()) {
      return "Size not fit.";
    }

    l1 = l1.stream().sorted().collect(Collectors.toList());
    l2 = l2.stream().sorted().collect(Collectors.toList());

    for (int i = 0; i < l1.size(); i++) {
      if (!l1.get(i).equals(l2.get(i))) {
        return String.format("Mismatch at %d, l1:%s, l2:%s", i, l1.get(i), l2.get(i));
      }
    }
    return null;
  }

  private static String printArrayAsOneLine(int[] array) {
    String result =
        Arrays.stream(array)
            .mapToObj(String::valueOf)
            .reduce((s1, s2) -> s1 + "\t" + s2)
            .orElse("");
    return result;
  }

  // endregion

}
