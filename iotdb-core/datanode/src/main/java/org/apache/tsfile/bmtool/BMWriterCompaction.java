package org.apache.tsfile.bmtool;

import org.apache.arrow.vector.Float8Vector;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FILE_NAME_SEPARATOR;

public class BMWriterCompaction {

  public static class DataSetsProfile {
    public static int deviceNum = 0;
    public static int seriesNum = 0;
    public static long ptsNum = 0;

    public static long singleSeriesLatency;
    public static long alignedSeriesLatency;
    public static long timeQueryLatency;
    public static long valueQueryLatency;
    public static long mixedQueryLatency;
  }

  public static boolean TO_PROFILE = true;

  public static String DST_DIR = "F:\\0006DataSets\\Results\\";
  public static String DATE_STR = java.time.format.DateTimeFormatter
      .ofPattern("ddHHmmss")
      .format(java.time.LocalDateTime.now());

  public static String NAME_COMMENT = "_NO_SPEC_";

  public static File tsFile;
  public static File logFile;

  public static TsFileWriter writer;
  public static BufferedWriter logger;

  public static int BATCH = 1024;
  public static CompressionType compressionType;
  public static TSEncoding encoding;

  public static void init() throws IOException {
    // tsFile = new File(FILE_PATH);
    logFile = new File(LOG_PATH);

    // writer = new TsFileWriter(tsFile);
    logger = new BufferedWriter(new FileWriter(logFile, true));
  }

  public static void closeAndSummary() throws IOException {
    writer.close();
    logger.write(String.format("Load: %s, Comment:%s, File:%s", CUR_DATA, NAME_COMMENT, tsFile.getName()));
    logger.newLine();
    logger.write(String.format("Pts: %d, Series: %d, Devs: %d, Arity: %d\n",
        DataSetsProfile.ptsNum, DataSetsProfile.seriesNum, DataSetsProfile.deviceNum,
        TSFileConfig.maxDegreeOfIndexNode));
    writer.report(logger);
    logger.write("==========================================");
    logger.newLine();
  }

  public static void bmTSBS() throws IOException, WriteProcessException {
    TSBSLoader loader = TSBSLoader.deserialize(CUR_DATA.getArrowFile());
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("lat", TSDataType.DOUBLE, encoding, compressionType));
    schemaList.add(new MeasurementSchema("lon", TSDataType.DOUBLE, encoding, compressionType));
    schemaList.add(new MeasurementSchema("ele", TSDataType.DOUBLE, encoding, compressionType));
    schemaList.add(new MeasurementSchema("vel", TSDataType.DOUBLE, encoding, compressionType));

    Tablet tablet = new Tablet("", schemaList, BATCH);
    tablet.initBitMaps();
    long[] timestamps = tablet.timestamps;
    double[] lats = (double[]) tablet.values[0];
    double[] lons = (double[]) tablet.values[1];
    double[] eles = (double[]) tablet.values[2];
    double[] vels = (double[]) tablet.values[3];

    String preDev = new String(loader.idVector.get(0), TSFileConfig.STRING_CHARSET);
    String curDev = null;
    tablet.setDeviceId(preDev);
    writer.registerAlignedTimeseries(new Path(preDev), schemaList);

    startDevTime.put(preDev, loader.timestampVector.get(0));

    int totalRows = loader.idVector.getValueCount();
    int rowInTablet = 0;
    long lastTS = -1L; // filter out-of-order data
    for (int cnt = 0; cnt < totalRows; cnt++) {
      curDev = new String(loader.idVector.get(cnt), TSFileConfig.STRING_CHARSET);
      if (cnt == totalRows/2) {
        writer.close();

        lastDevTime.put(preDev, loader.timestampVector.get(cnt - 1));
        resources.get(0).updateEndTime(lastDevTime);
        updateAllStartTime(resources.get(0), startDevTime);
        lastDevTime.clear();
        startDevTime.clear();
        resources.get(0).close();
        resources.get(0).serialize();

        startDevTime.put(curDev, loader.timestampVector.get(cnt));

        writer = getWriterFromResource(1);
        writer.registerAlignedTimeseries(new Path(curDev), schemaList);
      }

      if (!preDev.equals(curDev)) {
        lastDevTime.put(preDev, loader.timestampVector.get(cnt - 1));

        tablet.rowSize = rowInTablet;
        writer.writeAligned(tablet);
        tablet.reset();
        timestamps = tablet.timestamps;
        lats = (double[]) tablet.values[0];
        lons = (double[]) tablet.values[1];
        eles = (double[]) tablet.values[2];
        vels = (double[]) tablet.values[3];

        tablet.setDeviceId(curDev);
        writer.registerAlignedTimeseries(new Path(curDev), schemaList);
        rowInTablet = 0;

        startDevTime.put(curDev, loader.timestampVector.get(cnt));

        if (TO_PROFILE) {
          DataSetsProfile.deviceNum++;
          DataSetsProfile.seriesNum += 4;
        }

        preDev = curDev;
        lastTS = -1;
      }

      if (rowInTablet >= BATCH) {
        tablet.rowSize = BATCH;
        writer.writeAligned(tablet);
        tablet.reset();
        tablet.setDeviceId(curDev);
        lats = (double[]) tablet.values[0];
        lons = (double[]) tablet.values[1];
        eles = (double[]) tablet.values[2];
        vels = (double[]) tablet.values[3];
        rowInTablet = 0;
      }

      timestamps[rowInTablet] = loader.timestampVector.get(cnt);
      if (timestamps[rowInTablet] <= lastTS) {
        // rowInTablet not modified, next write is fine
        continue;
      }
      lastTS = timestamps[rowInTablet];
      setValueWithNull(lats, tablet.bitMaps[0], cnt, rowInTablet, loader.latVec);
      setValueWithNull(lons, tablet.bitMaps[1], cnt, rowInTablet, loader.lonVec);
      setValueWithNull(eles, tablet.bitMaps[2], cnt, rowInTablet, loader.eleVec);
      setValueWithNull(vels, tablet.bitMaps[3], cnt, rowInTablet, loader.velVec);

      rowInTablet ++;

      if (TO_PROFILE) {
        DataSetsProfile.ptsNum += 4;
      }
    }
    tablet.rowSize = rowInTablet;
    writer.writeAligned(tablet);

    lastDevTime.put(preDev, loader.timestampVector.get(totalRows-1));
    resources.get(1).updateEndTime(lastDevTime);
    updateAllStartTime(resources.get(1), startDevTime);
  }

  public static void setValueWithNull(double[] dvs, BitMap bm, int vecIdx, int tabIdx, Float8Vector vec) {
    if (vec.isNull(vecIdx)) {
      bm.mark(tabIdx);
    } else {
      dvs[tabIdx] = vec.get(vecIdx);
    }
  }

  public static void bmREDD() throws IOException, WriteProcessException {
    REDDLoader loader = REDDLoader.deserialize(CUR_DATA.getArrowFile());
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("elec", TSDataType.DOUBLE, encoding, compressionType));

    Tablet tablet = new Tablet("", schemaList, BATCH);
    long[] timestamps = tablet.timestamps;
    double[] elecs = (double[]) tablet.values[0];

    String preDev = new String(loader.idVector.get(0), TSFileConfig.STRING_CHARSET);
    String curDev = null;
    tablet.setDeviceId(preDev);
    writer.registerTimeseries(new Path(preDev), schemaList);

    startDevTime.put(preDev, loader.timestampVector.get(0));

    int totalRows = loader.idVector.getValueCount();
    int rowInTablet = 0;
    long lastTS = -1L; // filter out-of-order data
    for (int cnt = 0; cnt < totalRows; cnt++) {
      curDev = new String(loader.idVector.get(cnt), TSFileConfig.STRING_CHARSET);

      if (cnt == totalRows/2) {
        writer.close();

        lastDevTime.put(preDev, loader.timestampVector.get(cnt - 1));
        resources.get(0).updateEndTime(lastDevTime);
        updateAllStartTime(resources.get(0), startDevTime);
        lastDevTime.clear();
        startDevTime.clear();
        resources.get(0).close();
        resources.get(0).serialize();

        startDevTime.put(curDev, loader.timestampVector.get(cnt));

        writer = getWriterFromResource(1);
        writer.registerTimeseries(new Path(curDev), schemaList);
      }

      if (!preDev.equals(curDev)) {
        lastDevTime.put(preDev, loader.timestampVector.get(cnt-1));

        tablet.rowSize = rowInTablet;
        writer.write(tablet);
        tablet.reset();
        timestamps = tablet.timestamps;
        elecs = (double[]) tablet.values[0];
        tablet.setDeviceId(curDev);
        writer.registerTimeseries(new Path(curDev), schemaList);
        rowInTablet = 0;

        startDevTime.put(curDev, loader.timestampVector.get(cnt));

        if (TO_PROFILE) {
          DataSetsProfile.deviceNum++;
          DataSetsProfile.seriesNum += 1;
        }

        preDev = curDev;
        lastTS = -1;
      }

      if (rowInTablet >= BATCH) {
        tablet.rowSize = BATCH;
        writer.write(tablet);
        tablet.reset();
        tablet.setDeviceId(curDev);
        elecs = (double[]) tablet.values[0];
        rowInTablet = 0;
      }

      timestamps[rowInTablet] = loader.timestampVector.get(cnt);
      if (timestamps[rowInTablet] <= lastTS) {
        // rowInTablet not modified, next write is fine
        continue;
      }
      lastTS = timestamps[rowInTablet];
      elecs[rowInTablet] = loader.elecVector.get(cnt);
      rowInTablet ++;

      if (TO_PROFILE) {
        DataSetsProfile.ptsNum += 1;
      }
    }
    tablet.rowSize = rowInTablet;
    writer.write(tablet);

    lastDevTime.put(preDev, loader.timestampVector.get(totalRows - 1));
    resources.get(1).updateEndTime(lastDevTime);
    updateAllStartTime(resources.get(1), startDevTime);
  }

  public static void bmGeoLife() throws IOException, WriteProcessException {
    GeoLifeLoader loader = GeoLifeLoader.deserialize(CUR_DATA.getArrowFile());
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("lat", TSDataType.DOUBLE, encoding, compressionType));
    schemaList.add(new MeasurementSchema("lon", TSDataType.DOUBLE, encoding, compressionType));
    schemaList.add(new MeasurementSchema("alt", TSDataType.DOUBLE, encoding, compressionType));

    Tablet tablet = new Tablet("", schemaList, BATCH);
    long[] timestamps = tablet.timestamps;
    double[] lats = (double[]) tablet.values[0];
    double[] lons = (double[]) tablet.values[1];
    double[] alts = (double[]) tablet.values[2];

    String preDev = new String(loader.idVector.get(0), TSFileConfig.STRING_CHARSET);
    String curDev = null;
    tablet.setDeviceId(preDev);
    writer.registerTimeseries(new Path(preDev), schemaList);

    startDevTime.put(preDev, loader.timestampVector.get(0));

    int totalRows = loader.idVector.getValueCount();
    int rowInTablet = 0;
    long lastTS = -1L; // filter out-of-order data
    for (int cnt = 0; cnt < totalRows; cnt++) {
      curDev = new String(loader.idVector.get(cnt), TSFileConfig.STRING_CHARSET);
      // NOTE
      if (cnt == totalRows/2) {
        writer.close();

        lastDevTime.put(preDev, loader.timestampVector.get(cnt - 1));
        resources.get(0).updateEndTime(lastDevTime);
        updateAllStartTime(resources.get(0), startDevTime);
        lastDevTime.clear();
        startDevTime.clear();
        resources.get(0).close();
        resources.get(0).serialize();

        startDevTime.put(curDev, loader.timestampVector.get(cnt));

        writer = getWriterFromResource(1);
        writer.registerTimeseries(new Path(curDev), schemaList);
      }

      if (!preDev.equals(curDev)) {
        lastDevTime.put(preDev, loader.timestampVector.get(cnt-1));

        tablet.rowSize = rowInTablet;
        writer.write(tablet);
        tablet.reset();
        timestamps = tablet.timestamps;
        lats = (double[]) tablet.values[0];
        lons = (double[]) tablet.values[1];
        alts = (double[]) tablet.values[2];
        tablet.setDeviceId(curDev);
        writer.registerTimeseries(new Path(curDev), schemaList);
        rowInTablet = 0;

        startDevTime.put(curDev, loader.timestampVector.get(cnt));

        if (TO_PROFILE) {
          DataSetsProfile.deviceNum++;
          DataSetsProfile.seriesNum += 3;
        }

        preDev = curDev;
        lastTS = -1;
      }

      if (rowInTablet >= BATCH) {
        tablet.rowSize = BATCH;
        writer.write(tablet);
        tablet.reset();
        tablet.setDeviceId(curDev);
        lats = (double[]) tablet.values[0];
        lons = (double[]) tablet.values[1];
        alts = (double[]) tablet.values[2];
        rowInTablet = 0;
      }

      timestamps[rowInTablet] = loader.timestampVector.get(cnt);
      if (timestamps[rowInTablet] <= lastTS) {
        // rowInTablet not modified, next write is fine
        continue;
      }
      lastTS = timestamps[rowInTablet];
      lats[rowInTablet] = loader.latitudeVector.get(cnt);
      lons[rowInTablet] = loader.longitudeVector.get(cnt);
      alts[rowInTablet] = loader.altitudeVector.get(cnt);
      rowInTablet ++;

      if (TO_PROFILE) {
        DataSetsProfile.ptsNum += 3;
      }
    }
    tablet.rowSize = rowInTablet;
    writer.write(tablet);

    lastDevTime.put(preDev, loader.timestampVector.get(totalRows - 1));
    resources.get(1).updateEndTime(lastDevTime);
    updateAllStartTime(resources.get(1), startDevTime);
  }

  public static TDriveLoader bmTDrive() throws IOException, WriteProcessException {

    TDriveLoader loader = TDriveLoader.deserialize(CUR_DATA.getArrowFile());
    // loader.load(Long.MAX_VALUE);
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("lat", TSDataType.DOUBLE, encoding, compressionType));
    schemaList.add(new MeasurementSchema("lon", TSDataType.DOUBLE, encoding, compressionType));
    long clock;

    Tablet tablet = new Tablet("", schemaList, BATCH);
    long[] timestamps = tablet.timestamps;
    double[] lats = (double[]) tablet.values[0];
    double[] lons = (double[]) tablet.values[1];

    String preDev = new String(loader.idVector.get(0), TSFileConfig.STRING_CHARSET);
    String curDev = null;
    tablet.setDeviceId(preDev);
    writer.registerAlignedTimeseries(new Path(preDev), schemaList);

    startDevTime.put(preDev, loader.timestampVector.get(0)); // NOTE

    int totalRows = loader.idVector.getValueCount();
    int rowInTablet = 0;
    long lastTS = -1L; // filter out-of-order data
    for (int cnt = 0; cnt < totalRows; cnt++) {
      curDev = new String(loader.idVector.get(cnt), TSFileConfig.STRING_CHARSET);
      // NOTE
      if (cnt == totalRows/2) {
        writer.close();

        lastDevTime.put(preDev, loader.timestampVector.get(cnt - 1));
        resources.get(0).updateEndTime(lastDevTime);
        updateAllStartTime(resources.get(0), startDevTime);
        lastDevTime.clear();
        startDevTime.clear();
        resources.get(0).serialize();
        resources.get(0).close();

        startDevTime.put(curDev, loader.timestampVector.get(cnt));

        writer = getWriterFromResource(1);
        writer.registerAlignedTimeseries(new Path(curDev), schemaList);
      }

      if (!preDev.equals(curDev)) {
        lastDevTime.put(preDev, loader.timestampVector.get(cnt-1)); // NOTE

        tablet.rowSize = rowInTablet;
        writer.writeAligned(tablet);
        tablet.reset();
        timestamps = tablet.timestamps;
        lats = (double[]) tablet.values[0];
        lons = (double[]) tablet.values[1];
        tablet.setDeviceId(curDev);
        writer.registerAlignedTimeseries(new Path(curDev), schemaList);
        rowInTablet = 0;

        startDevTime.put(curDev, loader.timestampVector.get(cnt)); // NOTE

        if (TO_PROFILE) {
          DataSetsProfile.deviceNum++;
          DataSetsProfile.seriesNum += 2;
        }

        preDev = curDev;
        lastTS = -1;
      }

      if (rowInTablet >= BATCH) {
        tablet.rowSize = BATCH;
        writer.writeAligned(tablet);
        tablet.reset();
        tablet.setDeviceId(curDev);
        lats = (double[]) tablet.values[0];
        lons = (double[]) tablet.values[1];
        rowInTablet = 0;
      }

      timestamps[rowInTablet] = loader.timestampVector.get(cnt);
      if (timestamps[rowInTablet] <= lastTS) {
        // rowInTablet not modified, next write is fine
        continue;
      }
      lastTS = timestamps[rowInTablet];
      lats[rowInTablet] = loader.latitudeVector.get(cnt);
      lons[rowInTablet] = loader.longitudeVector.get(cnt);
      rowInTablet ++;

      if (TO_PROFILE) {
        DataSetsProfile.ptsNum += 2;
      }
    }
    tablet.rowSize = rowInTablet;
    writer.writeAligned(tablet);

    // NOTE
    lastDevTime.put(preDev, loader.timestampVector.get(totalRows - 1));
    resources.get(1).updateEndTime(lastDevTime);
    updateAllStartTime(resources.get(1), startDevTime);

    return loader;
  }

  public static void updateAllStartTime(TsFileResource resource, Map<String, Long> m) {
    for (Map.Entry<String, Long> entry : m.entrySet()) {
      resource.updateStartTime(entry.getKey(), 0);
    }
  }

  private static ICompactionPerformer performer = new FastCompactionPerformer(false);
  public static void compactUtilTDrive() throws Exception {
    FastCompactionTaskSummary summary=new FastCompactionTaskSummary();
    performer.setSummary(summary);
    CompactionTaskManager.getInstance().start();
    // TsFileResource targetResource =
    //     TsFileNameGenerator.getInnerCompactionTargetFileResource(resources, true);
    TsFileResource targetResource = new TsFileResource(
        new File(
            resources.get(0).getTsFile().getParent(),
            "compact-fast-" + CUR_DATA + ".tsfile"),
        TsFileResourceStatus.COMPACTING);
    performer.setSourceFiles(resources);
    performer.setTargetFiles(Collections.singletonList(targetResource));
    // performer.setSummary(new FastCompactionTaskSummary());
    long time = System.nanoTime();
    performer.perform();
    time = System.nanoTime() - time;
    CompactionTaskManager.getInstance().stop();
    report.append(String.format("Compaction %s by util for %d ms\n", CUR_DATA, time/1000000));
    System.out.println(String.format("Compaction %s by util for %d ms", CUR_DATA, time/1000000));
    System.out.println(summary);
  }

  private static ICompactionPerformer performer2 = new ReadPointCompactionPerformer();
  public static void compactWithNaive() throws Exception {
    CompactionTaskManager.getInstance().start();
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-Worker-1");
    // TsFileResource targetResource =
    //     TsFileNameGenerator.getInnerCompactionTargetFileResource(resources, true);
    TsFileResource targetResource = new TsFileResource(
        new File(
            resources.get(0).getTsFile().getParent(),
            "compact-naive-" + CUR_DATA + ".tsfile"),
        TsFileResourceStatus.COMPACTING);
    performer2.setSourceFiles(resources);
    performer2.setTargetFiles(Collections.singletonList(targetResource));
    performer2.setSummary(new FastCompactionTaskSummary());
    long time = System.nanoTime();
    performer2.perform();
    time = System.nanoTime() - time;
    CompactionTaskManager.getInstance().stop();
    report.append(String.format("Compaction %s naively for %d ms", CUR_DATA, time/1000000));
    System.out.println(String.format("Compaction %s naively for %d ms", CUR_DATA, time/1000000));
  }

  static Map<String, Long> lastDevTime = new HashMap<>();
  static Map<String, Long> startDevTime = new HashMap<>();
  static StringBuilder report = new StringBuilder();

  private static void commentOnName(String comment) {
    if (comment != null) {
      NAME_COMMENT = "_" + comment + "";
      FILE_PATH = DST_DIR + "TS_FILE_" + CUR_DATA + "_" + DATE_STR + NAME_COMMENT + ".tsfile";
      // LOG_PATH = DST_DIR + "TS_FILE_" + "_" + CUR_DATA + "_" + DATE_STR + NAME_COMMENT + ".log";
    }
  }

  public static void buildResources() {
    dataDirectory = new File(
        SRC_DIR
            + "0".concat(File.separator)
            + "0".concat(File.separator));
    for (int i = 1; i < 3; i++) {
      TsFileResource resource = new TsFileResource(
          new File(dataDirectory, String.format("%d-%d-0-0.tsfile", i, i)));
      resources.add(resource);
    }
  }

  public static void deserResrouces() throws IOException {
    dataDirectory = new File(
        SRC_DIR
            + "0".concat(File.separator)
            + "0".concat(File.separator));
    for (int i = 1; i < 3; i++) {
      TsFileResource resource = new TsFileResource(
          new File(dataDirectory, String.format("%d-%d-0-0.tsfile", i, i)));
      resource.deserialize();
      resources.add(resource);
    }
  }

  public static TsFileWriter getWriterFromResource(int i) throws IOException {
    tsFile = resources.get(i).getTsFile();
    return new TsFileWriter(new TsFileIOWriter(resources.get(i).getTsFile()));
  }

  private static File dataDirectory;

  public static List<TsFileResource> resources = new ArrayList<>();
  public static DataSets CUR_DATA = DataSets.REDD  ;
  public static String SRC_DIR = "F:\\0006DataSets\\ForCompact\\";
  public static String FILE_PATH = DST_DIR + "TS_FILE_" + CUR_DATA + "_" + DATE_STR + ".tsfile";
  public static String LOG_PATH = DST_DIR + "TS_FILE_Compaction_Results.log";
  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.out.println("ARGS too less");
    } else {
      CUR_DATA = DataSets.valueOf(args[0]);
    }

    // assign default
    compressionType = CompressionType.GZIP;
    encoding = TSEncoding.GORILLA;
    boolean makeComponents = true;
    boolean compactWithUtil = true;
    boolean compactNaive = true;

    // assign from args, args: [dataset, arity]
    if (args.length >= 2) {
      CUR_DATA = DataSets.valueOf(args[0]);
      TSFileConfig.maxDegreeOfIndexNode = Integer.parseInt(args[1]);
    } else {
      TSFileConfig.maxDegreeOfIndexNode = 256;
      System.out.println("Not Enough Arguments");
    }
    SRC_DIR = SRC_DIR + CUR_DATA.toString() + "\\";

    if (makeComponents) {
      buildResources();
    } else {
      deserResrouces();
    }
    init();

    long record = System.currentTimeMillis();
    // build components
    if (makeComponents) {
      writer = getWriterFromResource(0);
      switch (CUR_DATA) {
        case TDrive:
          bmTDrive();
          // bmTDrive(200);
          break;
        case GeoLife:
          bmGeoLife();
          break;
        case REDD:
          bmREDD();
          break;
        case TSBS:
          bmTSBS();
          break;
        default:
      }
      writer.close();
      resources.get(1).serialize();
      resources.get(1).close();
    }

    if (compactWithUtil) {
      compactUtilTDrive();
    }

    if (compactNaive) {
      compactWithNaive();
    }

    if (!makeComponents) {
      for (TsFileResource resource : resources) {
        resource.serialize();
        resource.close();
      }
    }


    // stopwatch for compacting

    record = System.currentTimeMillis() - record;
    System.out.println(String.format("Load and write for %d ms.", record));
    // closeAndSummary();
    // checker();
    System.out.println(report.toString());
    report.append("\n");
    logger.write(report.toString());
    logger.close();
  }
}
