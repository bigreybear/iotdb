package org.apache.iotdb.db.metadata.research;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.artree.ArtNode;
import org.apache.iotdb.db.metadata.artree.ArtTree;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.apache.iotdb.db.metadata.artree.ArtTree.deserialize;
import static org.apache.iotdb.db.metadata.artree.ArtTree.serialize;
import static org.apache.iotdb.db.metadata.artree.ArtTree.traverse;
import static org.apache.iotdb.db.metadata.artree.ArtTree.traverseAfterDeserialize;

public class Compare {
  private static final String FILE_PATH = "mtree_test".concat("TsFileIOWriterTest.tsfile");


  public static void main(String[] args) throws IOException, IllegalPathException {
    int totalLength;
    int testSize = 2000;

    Random random = new Random(System.currentTimeMillis());
    int randomStart = random.nextInt(600000);

    System.out.println("For adjacent paths.");
    List<String> paths = PathTextLoader.getPaths(randomStart, testSize);
    totalLength = paths.stream().mapToInt(String::length).sum();
    System.out.printf("Total length of original paths:%d%n", totalLength);
    testART(paths);
    long tsfile = testTsFile(paths);
    System.out.println(String.format("Cost %d bytes with %d paths with TsFile", tsfile, paths.size()));


    System.out.println("");
    System.out.println("For random paths.");
    paths = PathTextLoader.getRandomPaths(testSize);
    totalLength = paths.stream().mapToInt(String::length).sum();
    System.out.printf("Total length of original paths:%d%n", totalLength);
    testART(paths);
    tsfile = testTsFile(paths);
    System.out.println(String.format("Cost %d bytes with %d paths with TsFile", tsfile, paths.size()));
  }

  public static void testART(List<String> paths) throws IOException {
    ArtTree tree = new ArtTree();
    Map<String, Long> answerCheck = new HashMap<>();
    for (int i = 0; i < paths.size(); i++) {
      tree.insert(paths.get(i).getBytes(), (long) i);
      answerCheck.put(paths.get(i), (long) i);
    }
    ArtTree.calculateDepth(tree);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    serialize(((ArtNode)tree.root), baos);
    ReadWriteIOUtils.write(tree.root.offset, baos);
    System.out.println(String.format("Cost %d bytes with %d paths with ART", baos.size(), paths.size()));
    byte[] res = baos.toByteArray();
//    traverse((ArtNode) tree.root, "");
//    ArtNode r2 = (ArtNode) deserialize(ByteBuffer.wrap(res));
//    traverseAfterDeserialize(r2, "");
  }

  public static long testTsFile(List<String> paths) throws IOException, IllegalPathException {
    Map<String, Set<MeasurementSchema>> devAndSensirs = new HashMap<>();

    // parse paths
    PartialPath pp;
    for (String path : paths) {
      try {
        pp = new PartialPath(path);
      } catch (Exception e) {
        continue;
      }
      if (!devAndSensirs.containsKey(pp.getDevice())) {
        devAndSensirs.put(pp.getDevice(), new HashSet<>());
      }
      devAndSensirs
          .get(pp.getDevice())
          .add(
              new MeasurementSchema(pp.getMeasurement(), TSDataType.INT64, TSEncoding.RLE));
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

    return writer.metadataCost;
  }


  public static void writeChunkGroup2(TsFileIOWriter writer, String dev, Set<MeasurementSchema> schemas)
      throws IOException {
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

}
