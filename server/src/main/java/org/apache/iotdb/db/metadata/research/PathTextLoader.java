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
package org.apache.iotdb.db.metadata.research;

import org.apache.iotdb.db.metadata.path.PartialPath;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PathTextLoader {
  static String resultDir = "mtree_data";
  public static String fileName = "text_series.txt";

  public static void main2(String[] args) {
    List<String> res = readFileLines(resultDir + File.separator + fileName);
    res = res.stream().filter(PathTextLoader::containsNoChinese).collect(Collectors.toList());
    System.out.println(res);
  }

  public static void main(String[] args) {
    sortPathsByPrefix();
  }

  public static List<String> getAllPaths() {
    List<String> res = readFileLines(resultDir + File.separator + fileName);
    res = res.stream().filter(PathTextLoader::containsNoChinese).collect(Collectors.toList());
    return res;
  }

  private static void sortPathsByPrefix() {
    List<String> paths = getAllPaths();
    Map<String, Integer> devStatistic = new HashMap<>();
    PartialPath pp;
    String pre;
    for (String path : paths) {
      try {
        pp = new PartialPath(path);
      } catch (Exception e) {
        continue;
      }
      // define prefix index
      pre = pp.getPrefixString(4);
      devStatistic.merge(pre, 1, Integer::sum);
    }

    System.out.println(String.format("Total devices:%d", devStatistic.size()));

    devStatistic.entrySet().stream()
        .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
        .forEach(
            e -> {
              if (e.getValue() > 1000) {
                System.out.println(e.getKey());
              }
            });
  }

  public static List<String> getAdjacentPaths(int s, int size, boolean withChinese) {
    List<String> res = readFileLines(resultDir + File.separator + fileName);
    if (!withChinese) {
      res = res.stream().filter(PathTextLoader::containsNoChinese).collect(Collectors.toList());
    }

    if (s + size > res.size()) {
      return res.subList(res.size() - size, res.size());
    } else {
      return res.subList(s, s + size);
    }
  }

  public static List<String> getAdjacentPaths(int size, boolean withChinese) {
    Random r = new Random(System.currentTimeMillis());
    return getAdjacentPaths(r.nextInt(CompareSize.DATASET_SIZE), size, withChinese);
  }

  public static List<String> getAdjacentPathsWithoutChinese(int s, int size) {
    return getAdjacentPaths(s, size, false);
  }

  public static List<String> getRandomPathsWithoutChinese(int size) {
    return getRandomPaths(size, false);
  }

  public static List<String> getRandomPaths(int size, boolean withChinese) {
    List<String> res = readFileLines(resultDir + File.separator + fileName);
    if (!withChinese) {
      res = res.stream().filter(PathTextLoader::containsNoChinese).collect(Collectors.toList());
    }
    if (size > res.size()) {
      throw new IllegalArgumentException();
    }

    Collections.shuffle(res, new Random(System.currentTimeMillis()));
    return res.subList(0, size);
  }

  public static List<String> readFileLines(String filePath) {
    try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
      return stream.collect(Collectors.toList());
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  public static boolean containsNoChinese(String text) {
    String chineseCharacters = "[\u4E00-\u9FA5]";
    return !text.matches(".*" + chineseCharacters + ".*");
  }
}
