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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.pagemgr;

import org.apache.iotdb.commons.utils.TestOnly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A resource pool utilizing ConcurrentHashMap with reusable lock objects. todo generify for page
 * buffer management in further development.
 */
public class PageLockManager {
  private static class LockEntry {
    AtomicInteger refCnt = new AtomicInteger(0);
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    LockEntry() {}
  }

  private static final Logger logger = LoggerFactory.getLogger(PageLockManager.class);

  // initial amount of lock entries
  private static int INITIAL_ENTRY_SIZE = 100;
  // minimum spare size to stop recycling
  private static int MIN_SPARE_STOP_RECYCLE = 10;
  // maximum count to stop recycling for not being starving
  private static int MAX_RECYCLE_CNT = 60;

  private final Map<Integer, LockEntry> workingEntryTable = new ConcurrentHashMap<>();
  private final Deque<LockEntry> spareEntries = new ConcurrentLinkedDeque<>();
  private final AtomicBoolean recyclingIdleEntries = new AtomicBoolean(false);
  private final AtomicInteger totalLockCount = new AtomicInteger(INITIAL_ENTRY_SIZE);
  private final Set<Integer> suspectedIdleEntry =
      Collections.newSetFromMap(new ConcurrentHashMap<>());

  public PageLockManager() {
    for (int i = 0; i < INITIAL_ENTRY_SIZE; i++) {
      spareEntries.add(new LockEntry());
    }
  }

  @TestOnly
  public PageLockManager(int initEntrySize, int minSpareToStopRecycle, int maxCntToStopRecycle) {
    INITIAL_ENTRY_SIZE = initEntrySize;
    MIN_SPARE_STOP_RECYCLE = minSpareToStopRecycle;
    MAX_RECYCLE_CNT = maxCntToStopRecycle;
    totalLockCount.set(initEntrySize);
    for (int i = 0; i < INITIAL_ENTRY_SIZE; i++) {
      spareEntries.add(new LockEntry());
    }
  }

  public boolean tryWriteLock(int index) {
    LockEntry entry = getWorkingOrSpareEntry(index);
    if (!entry.lock.writeLock().tryLock()) {
      // lock is not used actually thus decrement the count
      entry.refCnt.decrementAndGet();
      return false;
    }
    suspectedIdleEntry.remove(index);
    return true;
  }

  public void lockWriteLock(int index) {
    suspectedIdleEntry.remove(index);
    LockEntry entry = getOrCreateEntry(index);
    entry.lock.writeLock().lock();
  }

  public void lockReadLock(int index) {
    suspectedIdleEntry.remove(index);
    LockEntry entry = getOrCreateEntry(index);
    entry.lock.readLock().lock();
  }

  public void unlockWriteLock(int index) {
    // unlock method can access entry directly since they cannot be recycled
    LockEntry entry = workingEntryTable.get(index);
    entry.lock.writeLock().unlock();
    if (entry.refCnt.decrementAndGet() == 0) {
      suspectedIdleEntry.add(index);
    }
  }

  public void unlockReadLock(int index) {
    LockEntry entry = workingEntryTable.get(index);
    entry.lock.readLock().unlock();
    if (entry.refCnt.decrementAndGet() == 0) {
      suspectedIdleEntry.add(index);
    }
  }

  private LockEntry getOrCreateEntry(int index) {
    LockEntry entry = getWorkingOrSpareEntry(index);
    return entry != null ? entry : recycleIdleOrCreateEntries(index);
  }

  private LockEntry getWorkingOrSpareEntry(int index) {
    // works as a key-level-concurrent map, synchronize on every key by compute method
    return workingEntryTable.compute(
        index,
        (k, v) -> {
          if (v != null || (v = spareEntries.poll()) != null) {
            // exclusively increment reference count before return the entry
            v.refCnt.incrementAndGet();
            return v;
          }
          return null;
        });
  }

  private LockEntry recycleIdleOrCreateEntries(int index) {
    LockEntry entry;
    // todo concurrent recycling might be explored further
    while (!recyclingIdleEntries.compareAndSet(false, true)) {
      if ((entry = getWorkingOrSpareEntry(index)) != null) {
        // in case evicting thread has recycled an entry
        return entry;
      }
      Thread.yield();
    }

    try {
      final AtomicInteger recycleCnt = new AtomicInteger(0);

      // suspected index is more likely to be recycled
      List<Integer> recycleIndex = new ArrayList<>(suspectedIdleEntry);
      recycleIndex.addAll(workingEntryTable.keySet());

      for (int i = 0; i < recycleIndex.size(); i++) {
        workingEntryTable.compute(
            recycleIndex.get(i),
            (k, v) -> {
              // v might be null if key is an invalid index from suspected set
              if (v != null && v.refCnt.get() == 0) {
                // the suspected set could be changing since it's concurrent
                suspectedIdleEntry.remove(k);
                spareEntries.add(v);
                recycleCnt.incrementAndGet();
                return null;
              }
              return v;
            });

        // CRITICAL condition to determine balance between scale and latency
        if (spareEntries.size() > MIN_SPARE_STOP_RECYCLE || recycleCnt.get() >= MAX_RECYCLE_CNT) {
          // WHEN spare entry size surpasses minimum threshold, the recycling thread shall proceed
          //  ASAP, and if this thread had recycled more entries than threshold it should proceed
          //  for not being starving.
          break;
        }
      }

      // true if another thread acquiring same index had got the entry in previous recycling
      if ((entry = getWorkingOrSpareEntry(index)) == null) {
        // no spare or idle entry thus no spinning-thread could proceed during this block
        entry = new LockEntry();
        // todo more strict constraint on lock amount
        totalLockCount.incrementAndGet();
        entry.refCnt.incrementAndGet();
        workingEntryTable.put(index, entry);
        // only when the new entry entered the table, could spinning-threads with same index proceed
      }
      return entry;
    } finally {
      recyclingIdleEntries.set(false);
    }
  }
}
