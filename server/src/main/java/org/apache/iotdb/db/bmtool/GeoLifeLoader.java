package org.apache.iotdb.db.bmtool;

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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.stream.Stream;

public class GeoLifeLoader {
  private static final Logger logger = LoggerFactory.getLogger(GeoLifeLoader.class);
  public static final boolean DEBUG = false;

  public static String DIR = "F:\\0006DataSets\\GeolifeTrajectories1.3\\GeolifeTrajectories1.3\\Data";
  public static String ARROW_DIR = "F:\\0006DataSets\\Arrows\\";

  private final BufferAllocator allocator;
  private VectorSchemaRoot root;
  public LargeVarCharVector idVector;
  public BigIntVector timestampVector;
  public Float8Vector latitudeVector;
  public Float8Vector longitudeVector;
  public Float8Vector altitudeVector;

  public static String tsFilePrefix = "root";

  final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public GeoLifeLoader() {
    this.allocator = new RootAllocator(4 * 1024 * 1024 * 1024L);

    // 定义 Schema
    Schema schema = new Schema(Arrays.asList(
        Field.nullable("id", FieldType.nullable(Types.MinorType.LARGEVARCHAR.getType()).getType()),
        Field.nullable("timestamp", FieldType.nullable(Types.MinorType.BIGINT.getType()).getType()),
        Field.nullable("latitude", FieldType.nullable(Types.MinorType.FLOAT8.getType()).getType()),
        Field.nullable("longitude", FieldType.nullable(Types.MinorType.FLOAT8.getType()).getType()),
        Field.nullable("altitude", FieldType.nullable(Types.MinorType.FLOAT8.getType()).getType())
    ));

    this.root = VectorSchemaRoot.create(schema, allocator);

    this.idVector = (LargeVarCharVector) root.getVector("id");
    this.timestampVector = (BigIntVector) root.getVector("timestamp");
    this.latitudeVector = (Float8Vector) root.getVector("latitude");
    this.longitudeVector = (Float8Vector) root.getVector("longitude");
    this.altitudeVector = (Float8Vector) root.getVector("altitude");
  }

  public GeoLifeLoader(RootAllocator ra, VectorSchemaRoot vsr) {
    this.allocator = ra;
    this.root = vsr;
    this.idVector = (LargeVarCharVector) root.getVector("id");
    this.timestampVector = (BigIntVector) root.getVector("timestamp");
    this.latitudeVector = (Float8Vector) root.getVector("latitude");
    this.longitudeVector = (Float8Vector) root.getVector("longitude");
    this.altitudeVector = (Float8Vector) root.getVector("altitude");
  }

  public void load(long limit) throws IOException {
    Files.walk(Paths.get(DIR))
        .filter(Files::isRegularFile)
        .filter(path -> path.toString().endsWith(".plt"))
        .limit(limit)
        .forEach(this::processPLTFile);
  }

  private static long fileCount = 0;
  private static final DateTimeFormatter GEOLIFE_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd,HH:mm:ss");
  public void processPLTFile(Path path) {
    if (DEBUG) {
      System.out.println(path.toAbsolutePath());
    }

    fileCount++;
    if (fileCount % 256 == 0) {
      System.out.println(path.toAbsolutePath());
    }

    try (Stream<String> lines = Files.lines(path).skip(6)) {
      lines.forEach(line -> {
        String[] parts = line.split(",");
        String id = "d" + path.getParent().getParent().getFileName().toString();
        double latitude = Double.parseDouble(parts[0]);
        double longitude = Double.parseDouble(parts[1]);
        double altitude = Double.parseDouble(parts[3]);
        LocalDateTime datetime = LocalDateTime.parse(parts[5] + "," + parts[6], GEOLIFE_DATE_TIME_FORMATTER);
        long timestamp = datetime.atZone(ZoneId.systemDefault()).toEpochSecond();
        // System.out.printf("ID: %s, Lat: %f, Lon: %f, Alt: %d, DateTime: %s%n",
        //     id, latitude, longitude, altitude, datetime);

        int pos = idVector.getValueCount();
        idVector.setValueCount(pos + 1);
        timestampVector.setValueCount(pos + 1);
        latitudeVector.setValueCount(pos + 1);
        altitudeVector.setValueCount(pos + 1);
        longitudeVector.setValueCount(pos + 1);

        idVector.setSafe(pos, new Text(id));
        timestampVector.setSafe(pos, timestamp);
        latitudeVector.setSafe(pos, latitude);
        altitudeVector.setSafe(pos, altitude);
        longitudeVector.setSafe(pos, longitude);
      });
    } catch (IOException e) {
      System.err.println("Error reading PLT file: " + path);
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
      double latitude = latitudeVector.get(i);
      double longitude = longitudeVector.get(i);
      double altitiude = altitudeVector.get(i);
      String formattedTimestamp = dateFormat.format(new java.util.Date(timestamp));
      System.out.println("Row " + i
          + ": ID=" + id
          + ", Timestamp=" + formattedTimestamp
          + ", Latitude=" + latitude
          + ", Longitude=" + longitude
          + ", Altitude=" + altitiude);
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

  public static GeoLifeLoader deserialize(String fileName) throws IOException{
    String filePath = ARROW_DIR + fileName;
    GeoLifeLoader loader = new GeoLifeLoader();
    RootAllocator allocator = new RootAllocator(4 * 1024 * 1024 * 1024L);
    try (FileInputStream fis = new FileInputStream(filePath);
         FileChannel channel = fis.getChannel();
         ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
      VectorSchemaRoot readRoot = reader.getVectorSchemaRoot();

      VarCharVector idVector = (VarCharVector) readRoot.getVector("id");
      BigIntVector timestampVector = (BigIntVector) readRoot.getVector("timestamp");
      Float8Vector latitudeVector = (Float8Vector) readRoot.getVector("latitude");
      Float8Vector longitudeVector = (Float8Vector) readRoot.getVector("longitude");
      Float8Vector altitudeVector = (Float8Vector) readRoot.getVector("altitude");

      while (reader.loadNextBatch()) {
        for (int i = 0; i < readRoot.getRowCount(); i++) {
          loader.idVector.setValueCount(i + 1);
          loader.timestampVector.setValueCount(i + 1);
          loader.latitudeVector.setValueCount(i + 1);
          loader.altitudeVector.setValueCount(i + 1);
          loader.longitudeVector.setValueCount(i + 1);

          loader.idVector.setSafe(i, new Text(idVector.get(i)));
          loader.timestampVector.setSafe(i, timestampVector.get(i));
          loader.latitudeVector.setSafe(i, latitudeVector.get(i));
          loader.altitudeVector.setSafe(i, altitudeVector.get(i));
          loader.longitudeVector.setSafe(i, longitudeVector.get(i));
        }
      }
      return loader;
    }
  }

  public static void main(String[] args) throws IOException {
    GeoLifeLoader loader = new GeoLifeLoader();
    // loader.load(20);
    loader.load(Long.MAX_VALUE);
    // loader.check(2000);
    loader.serializeTo("GeoLife.bin");
    // GeoLifeLoader loader1 = deserialize("GeoLifeDemo");
    // loader1.check(100);
    // loader1.close();
    loader.close();
  }
}
