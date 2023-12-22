package org.apache.iotdb.db.metadata.research;

import org.apache.iotdb.db.metadata.artree.ArtTree;
import org.apache.iotdb.db.metadata.path.PartialPath;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class PathHandler {
  // align paths to compare fairly, since TsFile only stores 1/256 sensors per device
  public static List<String> alignPathsWithTsMeta(List<String> oriPaths) {
    // device -> measurements
    Map<String, TreeSet<String>> m1 = new TreeMap<>();
    PartialPath pp;
    for (String p : oriPaths) {
      try {
        pp = new PartialPath(p);
        m1.computeIfAbsent(pp.getDevice(), k -> new TreeSet<>()).add(pp.getMeasurement());
      } catch (Exception e) {
      }
    }

    Iterator<String> ite;
    int cnt, cntTotal = 0;
    List<String> res = new ArrayList<>();
    // filter 1/256 measurements per device
    for (String device : m1.keySet()) {
      ite = m1.get(device).iterator();
      cnt = 0;
      while (ite.hasNext()) {
        ite.next();
        if (cnt % 256 != 0) {
          ite.remove();
        }
        cnt++;
      }
      cntTotal += cnt;

      ite = m1.get(device).iterator();
      while (ite.hasNext()) {
        res.add(device + "." + ite.next());
      }
    }

    System.out.println(
        String.format(
            "Remove %d series among %d devices as only 1/256 measurements per device could be stored.",
            cntTotal, m1.size()));
    return res;
  }

  // return all paths conforms to PartialPath syntax, a NECESSARY step for experiments
  public static List<String> filterPathSyntax(List<String> paths) {
    List<String> res = new ArrayList<>();
    int droppedSize = 0;
    for (String path : paths) {
      try {
        new PartialPath(path);
        // path conforms to syntax
        res.add(path);
      } catch (Exception e) {
        droppedSize++;
      }
    }
    System.out.println(
        String.format(
            "Paths after check:%d, dropped as illegal syntax :%d", res.size(), droppedSize));
    return res;
  }

  /**
   * BaoWu modeling many series suffixed VALUE which could degrade TsFile performance, cut it out to
   * make fair comparison. Syntax check is naturally included.
   */
  public static List<String> adjustWithBaoWuModeling(List<String> paths) {
    List<String> res = new ArrayList<>();
    PartialPath pp;
    int droppedSize = 0;
    for (String path : paths) {
      try {
        pp = new PartialPath(path);
        if (pp.getMeasurement().equals("value")) {
          droppedSize++;
          res.add(pp.getDevice());
        } else {
          res.add(path);
        }
      } catch (Exception e) {
      }
    }
    System.out.println(
        String.format("Adjust %d of %d paths for BaoWu modeling.", droppedSize, res.size()));
    return res;
  }

  public static List<String> checkPrefix(List<String> paths) {
    ArtTree tree = new ArtTree();
    List<String> res = new ArrayList<>(), excepts = new ArrayList<>();
    int c = 0;
    // SORTED to be commutative with alignWithTsMeta, which only reserves the first measurement per
    // device by order
    // if not SORTED, this check only reserves the first path with no prefix by its original order
    // consider 'FLOW1' and 'FLOW', ordered to be 'FLOW' and 'FLOW1',
    //  thus resulting different list if checkPrefix and alignWithTsMeta swap sequence.
    for (String p : paths.stream().sorted().collect(Collectors.toList())) {
      try {
        tree.insert(p.getBytes(), 1);
        res.add(p);
      } catch (Exception e) {
        excepts.add(p);
        c++;
      }
    }

    System.out.println(
        String.format("Remove %d among %d paths as prefix of others.", c, c + res.size()));
    return res;
  }
}
