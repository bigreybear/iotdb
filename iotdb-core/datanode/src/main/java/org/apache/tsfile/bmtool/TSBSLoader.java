package org.apache.tsfile.bmtool;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

public class TSBSLoader {
  private static final Logger logger = LoggerFactory.getLogger(TSBSLoader.class);
  public static final boolean DEBUG = false;

  private final BufferAllocator allocator;
  private VectorSchemaRoot root;
  public LargeVarCharVector idVector;
  public BigIntVector timestampVector;
  public Float8Vector latVec;
  public Float8Vector lonVec;
  public Float8Vector eleVec;
  public Float8Vector velVec;

  public static String tsFilePrefix = "root";

  final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public TSBSLoader() {
    this.allocator = new RootAllocator(16 * 1024 * 1024 * 1024L);

    // 定义 Schema
    Schema schema = new Schema(Arrays.asList(
        // id: fleet.name.driver
        Field.nullable("id", FieldType.nullable(Types.MinorType.LARGEVARCHAR.getType()).getType()),
        Field.nullable("timestamp", FieldType.nullable(Types.MinorType.BIGINT.getType()).getType()),
        Field.nullable("lat", FieldType.nullable(Types.MinorType.FLOAT8.getType()).getType()),
        Field.nullable("lon", FieldType.nullable(Types.MinorType.FLOAT8.getType()).getType()),
        Field.nullable("ele", FieldType.nullable(Types.MinorType.FLOAT8.getType()).getType()),
        Field.nullable("vel", FieldType.nullable(Types.MinorType.FLOAT8.getType()).getType())
    ));

    this.root = VectorSchemaRoot.create(schema, allocator);

    this.idVector = (LargeVarCharVector) root.getVector("id");
    this.timestampVector = (BigIntVector) root.getVector("timestamp");
    this.latVec = (Float8Vector) root.getVector("lat");
    this.lonVec = (Float8Vector) root.getVector("lon");
    this.eleVec = (Float8Vector) root.getVector("ele");
    this.velVec = (Float8Vector) root.getVector("vel");
  }


  public void load(long limit) throws IOException {
    Files.walk(Paths.get(DIR))
        .filter(Files::isRegularFile)
        .limit(limit)
        .forEach(p -> processDataFile(p, limit));
  }

  String[] p1,p2,p3;
  private static long fileCount = 0, collectedPoints = 0L;
  private static final DateTimeFormatter GEOLIFE_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd,HH:mm:ss");
  public void processDataFile(Path path, long limit) {
    if (DEBUG) {
      System.out.println(path.toAbsolutePath());
    }

    fileCount++;
    if (fileCount % 16 == 0) {
      System.out.println(path.toAbsolutePath());
    }

    try (Stream<String> lines = Files.lines(path).skip(4)) {
      lines.limit(limit).forEach(line -> {

        if (collectedPoints % 1_048_576 == 0) {
          System.out.println(collectedPoints);
        }

        String[] parts = line.split(",");
        if (collectedPoints > Integer.MAX_VALUE / 8) {

        } else if (parts[0].equals("diagnostics")) {
          curDev = null;
        } else if (parts[0].equals("tags")) {

          p1 = parts[1].split("=");
          p2 = parts[2].split("=");
          p3 = parts[3].split("=");
          if (p1.length < 2 || p2.length < 2 || p3.length < 2) {
            curDev = null;
          } else {
            curDev = String.format("%s.%s.%s",
                parts[2].split("=")[1],
                parts[1].split("=")[1],
                parts[3].split("=")[1]
            );
          }
        } else if (curDev != null) {

          if (!parts[0].equals("readings")) {
            System.out.println("WRONG DATA!!!!");;
          }

          long timestamp = Long.parseLong(parts[1]) / 1_000_000;
          if (!orderedPoints.containsKey(curDev) || orderedPoints.get(curDev).peekLast().ts < timestamp) {
            // only retain points NOT out-of-order
            orderedPoints.computeIfAbsent(curDev, k -> new ArrayDeque<>()).add(
                new TSBSPoint(
                    timestamp,
                    parseDoubleWithNull(parts[1]),
                    parseDoubleWithNull(parts[2]),
                    parseDoubleWithNull(parts[3]),
                    parseDoubleWithNull(parts[4])
            ));
            collectedPoints++;
          }
        }
      });
    } catch (IOException e) {
      System.err.println("Error reading REDD file: " + path);
      e.printStackTrace();
    }

  }

  public static double parseDoubleWithNull(String str) {
    if (str == null || str.length() == 0) {
      return Double.MAX_VALUE;
    }

    return Double.parseDouble(str);
  }

  public static String curDev;
  public static Map<String, ArrayDeque<TSBSPoint>> orderedPoints = new TreeMap<>();

  public static class TSBSPoint {
    long ts;
    double lat;
    double lon;
    double ele;
    double vel;
    public TSBSPoint(long t, double la, double lo, double el, double ve) {
      ts = t; lat = la; lon = lo; ele = el; vel = ve;
    }
  }

  public void close() {
    this.root.close();
    this.allocator.close();
  }

  public void check(int limit) {
    int numRows = Math.min(idVector.getValueCount(), limit);

    for (int i = 0; i < numRows; i++) {
      String id = new String(idVector.get(i), StandardCharsets.UTF_8);
      long timestamp = timestampVector.get(i);
      double lat = latVec.get(i);
      String formattedTimestamp = dateFormat.format(new java.util.Date(timestamp));
      System.out.println("Row " + i
          + ": ID=" + id
          + ", Timestamp=" + formattedTimestamp
          + ", Lat=" + lat);
    }
  }

  private void padVectors() {
    int totalRows = 0;
    Text keyText;
    for (Map.Entry<String, ArrayDeque<TSBSPoint>> entry : orderedPoints.entrySet()) {
      keyText = new Text(entry.getKey());
      for (TSBSPoint point : entry.getValue()) {
        idVector.setValueCount(totalRows + 1);
        timestampVector.setValueCount(totalRows + 1);
        latVec.setValueCount(totalRows + 1);
        lonVec.setValueCount(totalRows + 1);
        eleVec.setValueCount(totalRows + 1);
        velVec.setValueCount(totalRows + 1);

        idVector.setSafe(totalRows, keyText);
        timestampVector.setSafe(totalRows, point.ts);
        setWithNull(latVec, point.lat, totalRows);
        setWithNull(lonVec, point.lon, totalRows);
        setWithNull(eleVec, point.ele, totalRows);
        setWithNull(velVec, point.vel, totalRows);
        totalRows ++;
      }
    }
  }

  public static void setWithNull(FieldVector vec, double val, int index) {
    if (val == Double.MAX_VALUE) {
      vec.setNull(index);
    } else {
      ((Float8Vector)vec).setSafe(index, val);
    }
  }

  public void serializeTo(String fileName) throws IOException {
    padVectors();
    String filePath = ARROW_DIR + fileName; // 指定输出文件路径
    try (FileOutputStream fos = new FileOutputStream(filePath);
         FileChannel channel = fos.getChannel();
         ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
      root.setRowCount(idVector.getValueCount());
      writer.start();
      writer.writeBatch();
      writer.end();
    }
  }

  public static TSBSLoader deserialize(String path, String fileName) throws IOException {
    String filePath = path + fileName;
    TSBSLoader loader = new TSBSLoader();
    RootAllocator allocator = new RootAllocator(16 * 1024 * 1024 * 1024L);
    try (FileInputStream fis = new FileInputStream(filePath);
         FileChannel channel = fis.getChannel();
         ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
      VectorSchemaRoot readRoot = reader.getVectorSchemaRoot();

      LargeVarCharVector idVector = (LargeVarCharVector) readRoot.getVector("id");
      BigIntVector timestampVector = (BigIntVector) readRoot.getVector("timestamp");
      Float8Vector latVec = (Float8Vector) readRoot.getVector("lat");
      Float8Vector lonVec = (Float8Vector) readRoot.getVector("lon");
      Float8Vector eleVec = (Float8Vector) readRoot.getVector("ele");
      Float8Vector velVec = (Float8Vector) readRoot.getVector("vel");

      while (reader.loadNextBatch()) {
        for (int i = 0; i < readRoot.getRowCount(); i++) {
          loader.idVector.setValueCount(i + 1);
          loader.timestampVector.setValueCount(i + 1);
          loader.latVec.setValueCount(i + 1);
          loader.lonVec.setValueCount(i + 1);
          loader.eleVec.setValueCount(i + 1);
          loader.velVec.setValueCount(i + 1);

          loader.idVector.setSafe(i, new Text(idVector.get(i)));
          loader.timestampVector.setSafe(i, timestampVector.get(i));
          setWithNull(latVec, loader.latVec, i);
          setWithNull(lonVec, loader.lonVec, i);
          setWithNull(eleVec, loader.eleVec, i);
          setWithNull(velVec, loader.velVec, i);
        }
      }
      return loader;
    }
  }

  public static TSBSLoader deserialize(String fileName) throws IOException{
    return deserialize(ARROW_DIR, fileName);
  }

  public static void setWithNull(Float8Vector src, Float8Vector dst, int index) {
    if (src.isNull(index)) {
      dst.setNull(index);
    } else {
      dst.setSafe(index, src.get(index));
    }
  }

  public static String DIR = "F:\\0006DataSets\\TSBS2";
  public static String ARROW_DIR = "F:\\0006DataSets\\Arrows\\";

  public static void main(String[] args) throws IOException {
    TSBSLoader loader = new TSBSLoader();
    // loader.load(1000);
    loader.load(Long.MAX_VALUE);
    // loader.check(2000);
    loader.serializeTo(DataSets.TSBS.getArrowFile());
    // TSBSLoader loader1 = TSBSLoader.deserialize(DataSets.TSBS.getArrowFile());
    // loader1.check(100);
    // loader1.close();
    loader.close();
  }
}
