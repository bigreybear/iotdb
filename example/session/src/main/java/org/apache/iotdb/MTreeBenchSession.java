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

package org.apache.iotdb;

import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.pathgenerator.PathGenerator;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Pair;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class MTreeBenchSession {
  private static final int DEVICE_NUM = 30000;
  private static final int SENSOR_NUM = 50;
  private static final int TOTAL_SERIES = DEVICE_NUM * SENSOR_NUM;
  private static final int QUERY_NUM = 200;
  private static final double QUERY_RATIO = QUERY_NUM * 1.0 / TOTAL_SERIES;
  private static final boolean ONLY_READ = false;

  private static long timer_ingest;
  private static long timer_query;

  private static Session session;
  private static final String LOCAL_HOST = "127.0.0.1";

  private static Random RANDOM = new Random(2023);
  private static PathGenerator pg1;
  private static List<Pair<String, TSDataType>> answer = new ArrayList<>(QUERY_NUM);

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    session =
        new Session.Builder()
            .host(LOCAL_HOST)
            .port(6667)
            .username("root")
            .password("root")
            .version(Version.V_0_13)
            .build();
    session.open(false);

    // set session fetchSize
    session.setFetchSize(10000);

    // try {
    //   session.setStorageGroup("root.sg1");
    // } catch (StatementExecutionException e) {
    //   if (e.getStatusCode() != TSStatusCode.PATH_ALREADY_EXIST_ERROR.getStatusCode()) {
    //     throw e;
    //   }
    // }

    ingestAndMeasure();
    queryAndMeasure();
    querySpaceOverhead();
    // createMultiTimeseries();

    session.close();
    System.out.println(
        String.format("Spent %d mil-sec querying %d series.", timer_query, QUERY_NUM));
    System.out.println(
        String.format(
            "Spent %d mil-sec ingesting %dx%d=%d series.",
            timer_ingest, DEVICE_NUM, SENSOR_NUM, TOTAL_SERIES));
  }

  // fixme HACKED interface, only for ART/MTree compare
  private static void querySpaceOverhead()
      throws IoTDBConnectionException, StatementExecutionException {
    try (SessionDataSet dataSet = session.executeQueryStatement("count devices")) {
      RowRecord row = dataSet.next();
      System.out.println(String.format("Space overhead: %s", row.getFields().get(0).toString()));
    }
  }

  private static void queryAndMeasure()
      throws IoTDBConnectionException, StatementExecutionException {
    long timer;
    int alarm = 0;
    for (int i = 0; i < answer.size(); i++) {
      if (alarm * 8 > answer.size()) {
        System.out.println(
            String.format(
                "Now querying %d/%d series, %s.",
                i,
                answer.size(),
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));
        alarm = 0;
      }

      RowRecord row;
      timer = System.currentTimeMillis();
      try (SessionDataSet dataSet =
          session.executeQueryStatement(String.format("show timeseries %s;", answer.get(i).left))) {
        timer = System.currentTimeMillis() - timer;
        timer_query += timer;
        row = dataSet.next();
        if (!row.getFields().get(3).toString().equals(answer.get(i).right.toString())) {
          throw new StatementExecutionException("Answer wrong.");
        }
      }

      alarm++;
    }
  }

  private static void ingestAndMeasure()
      throws IoTDBConnectionException, StatementExecutionException {
    long timer;
    pg1 = new PathGenerator(DEVICE_NUM, SENSOR_NUM);
    pg1.generateAlignedPaths();

    List<Pair<String, List<String>>> res = pg1.getAlignedPaths();

    List<TSDataType> dataTypes = new ArrayList<>(SENSOR_NUM);
    List<TSEncoding> encodings = new ArrayList<>(Collections.nCopies(SENSOR_NUM, TSEncoding.RLE));
    List<CompressionType> compressionTypes =
        new ArrayList<>(Collections.nCopies(SENSOR_NUM, CompressionType.SNAPPY));
    List<String> alias = new ArrayList<>(Collections.nCopies(SENSOR_NUM, null));

    for (int i = 0; i < SENSOR_NUM; i++) {
      dataTypes.add(RANDOM.nextFloat() > 0.5 ? TSDataType.INT32 : TSDataType.INT64);
    }

    Collections.shuffle(res, RANDOM);

    int alarm = 0;
    for (int i = 0; i < DEVICE_NUM; i++) {
      Pair<String, List<String>> device = res.get(i);
      Collections.shuffle(device.right, RANDOM);
      Collections.shuffle(dataTypes, RANDOM);

      if (answer.size() < QUERY_NUM) {
        for (int j = 0; j < QUERY_RATIO * 1.2 * SENSOR_NUM; j++) {
          answer.add(
              new Pair<>(String.join(".", device.left, device.right.get(j)), dataTypes.get(j)));
        }
      }
      if (alarm * 8 > DEVICE_NUM && !ONLY_READ) {
        System.out.println(
            String.format(
                "Now ingesting %d/%d devices, %s.",
                i,
                DEVICE_NUM,
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));
        alarm = 0;
      }

      timer = System.currentTimeMillis();
      // System.out.println(device.left);
      if (!ONLY_READ) {
        session.createAlignedTimeseries(
            device.left, device.right, dataTypes, encodings, compressionTypes, null);
      }
      timer = System.currentTimeMillis() - timer;
      timer_ingest += timer;
      alarm++;
    }
    System.out.println(
        String.format(
            "Spent %d mil-sec ingesting %dx%d=%d series.",
            timer_ingest, DEVICE_NUM, SENSOR_NUM, TOTAL_SERIES));
  }

  private static void createMultiTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {

    if (!session.checkTimeseriesExists("root.sg1.d2.s1")
        && !session.checkTimeseriesExists("root.sg1.d2.s2")) {
      List<String> paths = new ArrayList<>();
      paths.add("root.sg1.d2.s1");
      paths.add("root.sg1.d2.s2");
      List<TSDataType> tsDataTypes = new ArrayList<>();
      tsDataTypes.add(TSDataType.INT64);
      tsDataTypes.add(TSDataType.INT64);
      List<TSEncoding> tsEncodings = new ArrayList<>();
      tsEncodings.add(TSEncoding.RLE);
      tsEncodings.add(TSEncoding.RLE);
      List<CompressionType> compressionTypes = new ArrayList<>();
      compressionTypes.add(CompressionType.SNAPPY);
      compressionTypes.add(CompressionType.SNAPPY);

      List<Map<String, String>> tagsList = new ArrayList<>();
      Map<String, String> tags = new HashMap<>();
      tags.put("unit", "kg");
      tagsList.add(tags);
      tagsList.add(tags);

      List<Map<String, String>> attributesList = new ArrayList<>();
      Map<String, String> attributes = new HashMap<>();
      attributes.put("minValue", "1");
      attributes.put("maxValue", "100");
      attributesList.add(attributes);
      attributesList.add(attributes);

      List<String> alias = new ArrayList<>();
      alias.add("weight1");
      alias.add("weight2");

      session.createMultiTimeseries(
          paths, tsDataTypes, tsEncodings, compressionTypes, null, tagsList, attributesList, alias);
    }
  }
}
