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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.apache.tsfile.bmtool.GeoLifeLoader.ARROW_DIR;

public class TDriveLoader {
  private static final Logger logger = LoggerFactory.getLogger(TDriveLoader.class);

  public static String DIR = "F:\\0006DataSets\\TDrive\\";

  private final BufferAllocator allocator;
  private VectorSchemaRoot root;
  public LargeVarCharVector idVector;
  public BigIntVector timestampVector;
  public Float8Vector latitudeVector;
  public Float8Vector longitudeVector;

  public static String tsFilePrefix = "root";

  final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public TDriveLoader() {
    this.allocator = new RootAllocator(2 * 1024 * 1024 * 1024L);

    // 定义 Schema
    Schema schema = new Schema(Arrays.asList(
        Field.nullable("id", FieldType.nullable(Types.MinorType.LARGEVARCHAR.getType()).getType()),
        Field.nullable("timestamp", FieldType.nullable(Types.MinorType.BIGINT.getType()).getType()),
        Field.nullable("latitude", FieldType.nullable(Types.MinorType.FLOAT8.getType()).getType()),
        Field.nullable("longitude", FieldType.nullable(Types.MinorType.FLOAT8.getType()).getType())
    ));

    this.root = VectorSchemaRoot.create(schema, allocator);

    this.idVector = (LargeVarCharVector) root.getVector("id");
    this.timestampVector = (BigIntVector) root.getVector("timestamp");
    this.latitudeVector = (Float8Vector) root.getVector("latitude");
    this.longitudeVector = (Float8Vector) root.getVector("longitude");
  }

  public void load(long limit) throws IOException {
    try (Stream<Path> stream = Files.walk(Paths.get(DIR))) {
      stream
          .filter(path -> path.toString().endsWith(".txt"))
          .limit(limit)
          .forEach(p -> {
            String folderName = p.getParent().getFileName().toString();
            try (Stream<String> lines = Files.lines(p)) {
              lines.forEach(line -> {
                try {
                  String[] parts = line.split(",");
                  // String id = tsFilePrefix + "." + folderName + "_" + parts[0];
                  String id = folderName + "_" + parts[0];
                  long timestamp = dateFormat.parse(parts[1]).getTime();
                  double latitude = Double.parseDouble(parts[2]);
                  double longitude = Double.parseDouble(parts[3]);

                  int pos = idVector.getValueCount();
                  idVector.setValueCount(pos + 1);
                  timestampVector.setValueCount(pos + 1);
                  latitudeVector.setValueCount(pos + 1);
                  longitudeVector.setValueCount(pos + 1);

                  idVector.setSafe(pos, new Text(id));
                  timestampVector.setSafe(pos, timestamp);
                  latitudeVector.setSafe(pos, latitude);
                  longitudeVector.setSafe(pos, longitude);
                } catch (ParseException e) {
                  System.err.println("Error parsing date: " + e.getMessage());
                }
              });
            } catch (IOException e) {
              System.err.println("Error reading file: " + p);
            }
          });
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
      String formattedTimestamp = dateFormat.format(new java.util.Date(timestamp));
      System.out.println("Row " + i + ": ID=" + id + ", Timestamp=" + formattedTimestamp + ", Latitude=" + latitude + ", Longitude=" + longitude);
    }
  }

  public void serializeTo(String fileName) throws IOException {
    String filePath = ARROW_DIR + fileName;
    try (FileOutputStream fos = new FileOutputStream(filePath);
         FileChannel channel = fos.getChannel();
         ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
      root.setRowCount(idVector.getValueCount());
      writer.start();
      writer.writeBatch();
      writer.end();
    }
  }

  public static TDriveLoader deserialize(String fileName) throws IOException{
    String filePath = ARROW_DIR + fileName;
    TDriveLoader loader = new TDriveLoader();
    RootAllocator allocator = new RootAllocator(4 * 1024 * 1024 * 1024L);
    try (FileInputStream fis = new FileInputStream(filePath);
         FileChannel channel = fis.getChannel();
         ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
      VectorSchemaRoot readRoot = reader.getVectorSchemaRoot();

      VarCharVector idVector = (VarCharVector) readRoot.getVector("id");
      BigIntVector timestampVector = (BigIntVector) readRoot.getVector("timestamp");
      Float8Vector latitudeVector = (Float8Vector) readRoot.getVector("latitude");
      Float8Vector longitudeVector = (Float8Vector) readRoot.getVector("longitude");

      while (reader.loadNextBatch()) {
        for (int i = 0; i < readRoot.getRowCount(); i++) {
          loader.idVector.setValueCount(i + 1);
          loader.timestampVector.setValueCount(i + 1);
          loader.latitudeVector.setValueCount(i + 1);
          loader.longitudeVector.setValueCount(i + 1);

          loader.idVector.setSafe(i, new Text(idVector.get(i)));
          loader.timestampVector.setSafe(i, timestampVector.get(i));
          loader.latitudeVector.setSafe(i, latitudeVector.get(i));
          loader.longitudeVector.setSafe(i, longitudeVector.get(i));
        }
      }
      return loader;
    }
  }

  public static void main(String[] args) throws IOException {
    TDriveLoader loader = new TDriveLoader();
    // loader.load(Long.MAX_VALUE);
    // loader.serializeTo("TDrive.bin");
    // loader.check(2000);
    TDriveLoader loader1 = TDriveLoader.deserialize(DataSets.TDrive.getArrowFile());
    loader1.close();
    loader.close();
  }
}
