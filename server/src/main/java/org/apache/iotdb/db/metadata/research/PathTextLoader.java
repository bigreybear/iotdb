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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PathTextLoader {
  static String resultDir = "mtree_data";

  public static void main(String[] args) {
    List<String> res = readFileLines(resultDir + File.separator + "text_series.txt");
    res = res.stream().filter(PathTextLoader::containsNoChinese).collect(Collectors.toList());
    System.out.println(res);
  }

  public static List<String> getAllPaths() {
    List<String> res = readFileLines(resultDir + File.separator + "text_series.txt");
    res = res.stream().filter(PathTextLoader::containsNoChinese).collect(Collectors.toList());
    return res;
  }

  public static List<String> getPaths(int s, int size) {
    List<String> res = readFileLines(resultDir + File.separator + "text_series.txt");
    res = res.stream().filter(PathTextLoader::containsNoChinese).collect(Collectors.toList());

    if (s + size > res.size()) {
      return res.subList(res.size() - size, res.size());
    } else {
      return res.subList(s, s+size);
    }
  }

  public static List<String> getRandomPaths(int size) {
    List<String> res = readFileLines(resultDir + File.separator + "text_series.txt");
    res = res.stream().filter(PathTextLoader::containsNoChinese).collect(Collectors.toList());
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
