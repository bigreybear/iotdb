package org.apache.iotdb.db.metadata.research;

import org.apache.iotdb.db.bmtool.DataSets;
import org.apache.iotdb.db.bmtool.GeoLifeLoader;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.artree.ArtTree;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CompareReadForPaper {
  private static final String FILE_PATH = "mtree_test".concat("_TsFileIOWriterTest.tsfile");

  public static DataSets DATA_SETS = DataSets.REDD;

  public static void main(String[] args) throws IOException, IllegalPathException {
    // for paper
    long res1, res2;
    List<String> devs = CompareSizeAndSpeed.getDevsFromArrow(DATA_SETS);
    int base = prepareTsFile(devs);
    devs = PathHandler.alignPathsWithTsMetaForRead(devs);
    ArtTree tree = prepareARTFile(devs);
    MockARTFileReader mart = new MockARTFileReader(base, tree);
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH);

    List<String> qdevs = new ArrayList<>();
    List<String> qmeas = new ArrayList<>();
    List<String> qnewPath = new ArrayList<>();

    for (String path : devs) {
      try{
        PartialPath pp = new PartialPath(path);
        qdevs.add(pp.getDevice());
        qmeas.add(pp.getMeasurement());
        qnewPath.add(path);
      } catch (Exception e) {

      }
    }

    res1 = System.nanoTime();
    List<Long> res = new ArrayList<>(devs.size());
    for (int i = 0; i < qdevs.size(); i++) {
      res.add(reader.getOffsetByPath(qdevs.get(i), qmeas.get(i)));
    }
    res1 = System.nanoTime() - res1;

    res2 = System.nanoTime();
    res.clear();
    for (int i = 0; i < qnewPath.size(); i++) {
      res.add(mart.getValueByChannel(qnewPath.get(i)));
    }
    res2 = System.nanoTime() - res2;

    reader.close();
    mart.close();

    System.out.println(DATA_SETS);
    System.out.println(qdevs.size());
    // System.out.println(qnewPath.size());
    System.out.println(res1);
    System.out.println(res2);
  }

  public static void main2(String[] args) throws Exception {
    long res1 = 0L, res2 = 0L;

    List<String> p = PathTextLoader.getAdjacentPaths(0, 5000, false);
    // prepare files
    p = PathHandler.filterPathSyntax(p);
    int base = prepareTsFile(p);
    p = PathHandler.alignPathsWithTsMeta(p);
    p = PathHandler.checkPrefix(p);
    ArtTree tree = prepareARTFile(p);
    MockARTFileReader mart = new MockARTFileReader(base, tree);
    TsFileSequenceReader reader = new TsFileSequenceReader(FILE_PATH);

    List<String> devs = new ArrayList<>();
    List<String> meas = new ArrayList<>();
    List<String> newPath = new ArrayList<>();
    for (String path : p) {
      try{
        PartialPath pp = new PartialPath(path);
        devs.add(pp.getDevice());
        meas.add(pp.getMeasurement());
        newPath.add(path);
      } catch (Exception e) {

      }
    }

    res1 = System.currentTimeMillis();
    List<Long> res = new ArrayList<>(p.size());
    for (int i = 0; i < devs.size(); i++) {
      res.add(reader.getOffsetByPath(devs.get(i), meas.get(i)));
    }
    res1 = System.currentTimeMillis() - res1;

    res2 = System.currentTimeMillis();
    res.clear();
    for (int i = 0; i < newPath.size(); i++) {
      res.add(mart.getValueByChannel(newPath.get(i)));
    }
    res2 = System.currentTimeMillis() - res2;

    reader.close();
    mart.close();

    System.out.println(res1);
    System.out.println(res2);
  }

  public static ArtTree prepareARTFile(List<String> paths) throws IOException {
    ArtTree tree = new ArtTree();
    Map<String, Long> answerCheck = new HashMap<>();
    for (int i = 0; i < paths.size(); i++) {
      tree.insert(paths.get(i).getBytes(), (long) i);
      // answerCheck.put(paths.get(i), (long) i);
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    tree.collectStatistics();
    return tree;
  }

  public static int prepareTsFile(List<String> paths) throws IOException, IllegalPathException {
    Map<String, Set<MeasurementSchema>> devAndSensirs = new HashMap<>();

    // parse paths
    for (String path : paths) {
      // pp = new PartialPath(path);
      if (!devAndSensirs.containsKey(path)) {
        devAndSensirs.put(path, new HashSet<>());
      }

      for (String sensor : DATA_SETS.sensors) {
        devAndSensirs
            .get(path)
            .add(new MeasurementSchema(sensor, TSDataType.INT64, TSEncoding.RLE));
      }
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

    return (int) writer.dataSize;
  }

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
}
