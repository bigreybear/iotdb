package org.apache.tsfile.bmtool;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarCharVector;
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
import java.util.Arrays;
import java.util.stream.Stream;

public class REDDLoader {
  private static final Logger logger = LoggerFactory.getLogger(REDDLoader.class);
  public static final boolean DEBUG = false;

  public static String DIR = "F:\\0006DataSets\\REDD";
  public static String ARROW_DIR = "F:\\0006DataSets\\Arrows\\";

  private final BufferAllocator allocator;
  private VectorSchemaRoot root;
  public LargeVarCharVector idVector;
  public BigIntVector timestampVector;
  public Float8Vector elecVector;
  public Float8Vector longitudeVector;
  public Float8Vector altitudeVector;

  public static String tsFilePrefix = "root";

  final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public REDDLoader() {
    this.allocator = new RootAllocator(16 * 1024 * 1024 * 1024L);

    // 定义 Schema
    Schema schema = new Schema(Arrays.asList(
        Field.nullable("id", FieldType.nullable(Types.MinorType.LARGEVARCHAR.getType()).getType()),
        Field.nullable("timestamp", FieldType.nullable(Types.MinorType.BIGINT.getType()).getType()),
        Field.nullable("elec", FieldType.nullable(Types.MinorType.FLOAT8.getType()).getType())
    ));

    this.root = VectorSchemaRoot.create(schema, allocator);

    this.idVector = (LargeVarCharVector) root.getVector("id");
    this.timestampVector = (BigIntVector) root.getVector("timestamp");
    this.elecVector = (Float8Vector) root.getVector("elec");
  }


  public void load(long limit) throws IOException {
    Files.walk(Paths.get(DIR))
        .filter(Files::isRegularFile)
        .limit(limit)
        .forEach(this::processDataFile);
  }

  private static long fileCount = 0;
  private static final DateTimeFormatter GEOLIFE_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd,HH:mm:ss");
  public void processDataFile(Path path) {
    if (DEBUG) {
      System.out.println(path.toAbsolutePath());
    }

    fileCount++;
    if (fileCount % 16 == 0) {
      System.out.println(path.toAbsolutePath());
    }

    try (Stream<String> lines = Files.lines(path).skip(1)) {
      lines.forEach(line -> {
        String[] pathNodes = path.getFileName().toString().split("\\.");
        String id = pathNodes[0] + "." + pathNodes[2];

        String[] parts = line.split(",");
        double elec = Double.parseDouble(parts[2]);
        long timestamp = Long.parseLong(parts[1]) / 1_000_000;
        // System.out.printf("ID: %s, Lat: %f, Lon: %f, Alt: %d, DateTime: %s%n",
        //     id, latitude, longitude, altitude, datetime);

        int pos = idVector.getValueCount();
        idVector.setValueCount(pos + 1);
        timestampVector.setValueCount(pos + 1);
        elecVector.setValueCount(pos + 1);

        idVector.setSafe(pos, new Text(id));
        timestampVector.setSafe(pos, timestamp);
        elecVector.setSafe(pos, elec);
      });
    } catch (IOException e) {
      System.err.println("Error reading REDD file: " + path);
      e.printStackTrace();
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
      double elec = elecVector.get(i);
      String formattedTimestamp = dateFormat.format(new java.util.Date(timestamp));
      System.out.println("Row " + i
          + ": ID=" + id
          + ", Timestamp=" + formattedTimestamp
          + ", Elec=" + elec);
    }
  }

  public void serializeTo(String fileName) throws IOException {
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

  public static REDDLoader deserialize(String fileName) throws IOException{
    String filePath = ARROW_DIR + fileName;
    REDDLoader loader = new REDDLoader();
    RootAllocator allocator = new RootAllocator(4 * 1024 * 1024 * 1024L);
    try (FileInputStream fis = new FileInputStream(filePath);
         FileChannel channel = fis.getChannel();
         ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
      VectorSchemaRoot readRoot = reader.getVectorSchemaRoot();

      VarCharVector idVector = (VarCharVector) readRoot.getVector("id");
      BigIntVector timestampVector = (BigIntVector) readRoot.getVector("timestamp");
      Float8Vector latitudeVector = (Float8Vector) readRoot.getVector("elec");

      while (reader.loadNextBatch()) {
        for (int i = 0; i < readRoot.getRowCount(); i++) {
          loader.idVector.setValueCount(i + 1);
          loader.timestampVector.setValueCount(i + 1);
          loader.elecVector.setValueCount(i + 1);

          loader.idVector.setSafe(i, new Text(idVector.get(i)));
          loader.timestampVector.setSafe(i, timestampVector.get(i));
          loader.elecVector.setSafe(i, latitudeVector.get(i));
        }
      }
      return loader;
    }
  }

  public static void main(String[] args) throws IOException {
    REDDLoader loader = new REDDLoader();
    // loader.load(20);
    loader.load(Long.MAX_VALUE);
    // loader.check(2000);
    loader.serializeTo(DataSets.REDD.getArrowFile());
    REDDLoader loader1 = REDDLoader.deserialize(DataSets.REDD.getArrowFile());
    loader1.check(100);
    loader1.close();
    loader.close();
  }
}
