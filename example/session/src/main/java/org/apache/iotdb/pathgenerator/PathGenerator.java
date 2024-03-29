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

package org.apache.iotdb.pathgenerator;

import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;

// import com.ankurdave.part.ArtTree;
// import com.github.rohansuri.art.AdaptiveRadixTree;
// import com.github.rohansuri.art.BinaryComparables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class PathGenerator {

  public static final Random RANDOM = new Random(1211);
  private static final int MAX_DEVICE_SUFFIX = 99999999;

  private int expectedSize, devSize, sensorSize;
  private String[] paths;
  private List<Pair<String, List<String>>> aligendPaths;

  public PathGenerator() {}

  public PathGenerator(int devSize, int sensorSize) {
    this.devSize = devSize;
    this.sensorSize = sensorSize;
    expectedSize = devSize * sensorSize;
  }

  public Iterator<String> getIte() {
    return Arrays.asList(paths).iterator();
  }

  public static String getStringNumber(int width, int value) {
    return String.format("%0" + width + "d", value);
  }

  private static List<String> getRandomSensors(int size) {
    List<String> elems =
        Arrays.stream(MeasurementName.values())
            .map(MeasurementName::toString)
            .collect(Collectors.toList());
    Collections.shuffle(elems, RANDOM);
    return elems.subList(0, size);
  }

  private static <T extends Enum<?>> T getRandomEnum(Class<T> enumClass) {
    return enumClass.getEnumConstants()[RANDOM.nextInt(enumClass.getEnumConstants().length)];
  }

  public List<Pair<String, List<String>>> getAlignedPaths() {
    return aligendPaths;
  }

  public void generateAlignedPaths() {
    aligendPaths = new ArrayList<>(expectedSize);
    long timer = System.currentTimeMillis();
    int cnt = 0;
    String[] pathComp = new String[3];
    pathComp[0] = "root";
    pathComp[1] = getRandomEnum(L1Prefix.class).toString();

    StringBuilder part2Builder = new StringBuilder(getRandomEnum(L2Prefix.class).toString());
    int builderLength = part2Builder.length();
    int devNumber = RANDOM.nextInt(MAX_DEVICE_SUFFIX);
    List<String> sensors = getRandomSensors(sensorSize);

    while (cnt < expectedSize) {
      pathComp[2] = part2Builder.append(getStringNumber(8, devNumber)).toString();
      // padding result
      aligendPaths.add(new Pair<>(String.join(".", pathComp), sensors));
      cnt += sensorSize;

      part2Builder.setLength(builderLength);
      devNumber = devNumber == MAX_DEVICE_SUFFIX ? 0 : devNumber + 1;

      if (RANDOM.nextFloat() > 0.95) {
        pathComp[1] = getRandomEnum(L1Prefix.class).toString();
        part2Builder.setLength(0);
        part2Builder.append(getRandomEnum(L2Prefix.class).toString());
        builderLength = part2Builder.length();
        sensors = getRandomSensors(sensorSize);
      }
    }

    timer = System.currentTimeMillis() - timer;
    System.out.println(String.format("Generating aligned series for %d mil-seconds", timer));
  }

  public void generate() {
    paths = new String[expectedSize];
    long timer = System.currentTimeMillis();
    int cnt = 0;
    String[] pathComp = new String[4];
    pathComp[0] = "root";

    pathComp[1] = getRandomEnum(L1Prefix.class).toString();

    StringBuilder sb = new StringBuilder(getRandomEnum(L2Prefix.class).toString());
    int sbLength = sb.length();
    int devNumber = RANDOM.nextInt(MAX_DEVICE_SUFFIX);

    List<String> sensors = getRandomSensors(sensorSize);

    while (cnt < expectedSize) {
      pathComp[2] = sb.append(getStringNumber(8, devNumber)).toString();
      for (String sensor : sensors) {
        pathComp[3] = sensor;
        paths[cnt] = String.join(".", pathComp);
        cnt++;
      }
      sb.setLength(sbLength);
      devNumber = devNumber == MAX_DEVICE_SUFFIX ? 0 : devNumber + 1;

      if (RANDOM.nextFloat() > 0.95) {
        pathComp[1] = getRandomEnum(L1Prefix.class).toString();
        sb.setLength(0);
        sb.append(getRandomEnum(L2Prefix.class).toString());
        sbLength = sb.length();
        sensors = getRandomSensors(sensorSize);
      }
    }

    timer = System.currentTimeMillis() - timer;
    System.out.println(String.format("Generating for %d mil-seconds", timer));
  }

  public String[] selectedPaths(int size) {
    String[] res = new String[size];
    for (int i = 0; i < size; i++) {
      res[i] = paths[RANDOM.nextInt(paths.length)];
    }
    return res;
  }

  public static void main(String[] args) {
    // Random seed = new Random();
    // int mSize = MeasurementName.values().length;
    // int chosen = seed.nextInt(mSize);
    // System.out.println(MeasurementName.values()[chosen]);
    // System.out.println(getRandomEnum(MeasurementName.class));
    // System.out.println("hellp");
    //
    // System.out.println(getStringNumber(8, chosen));
    // System.out.println(PathGenerator.getRandomSensors(20));

    // for (int a = 0; a < 55; a++) {
    //     System.out.println(RANDOM.nextFloat());
    // }

    PathGenerator pg1 = new PathGenerator(4000, 25);
    pg1.generate();
    pg1.generateAlignedPaths();
    List<Pair<String, List<String>>> res = pg1.getAlignedPaths();

    System.out.println("over");
    // Iterator<String> res = pg1.getIte();
    // while (res.hasNext()) {
    //     System.out.println(res.next());
    // }

    // ArtTree ankurTree = new ArtTree();
    // AdaptiveRadixTree<String, String> rohanTree =
    //     new AdaptiveRadixTree<>(BinaryComparables.forString());
    //
    // for (String str : pg1.paths) {
    //   ankurTree.insert(str.getBytes(), str);
    //   rohanTree.put(str, str);
    // }
    //
    // System.out.println(RamUsageEstimator.sizeOf(ankurTree));
    // System.out.println(RamUsageEstimator.sizeOf(rohanTree));

    // Iterator<Tuple2<byte[], Object>> ite = at.iterator();
    // while (ite.hasNext()) {
    //   System.out.println(new String(ite.next()._1));
    // }
  }
}
