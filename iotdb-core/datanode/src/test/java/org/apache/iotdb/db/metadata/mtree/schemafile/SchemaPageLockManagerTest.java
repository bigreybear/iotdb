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
package org.apache.iotdb.db.metadata.mtree.schemafile;

import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.pagemgr.PageLockManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SchemaPageLockManagerTest {
  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testConcurrentAccess() throws InterruptedException {
    int[] sharedData = new int[200];
    int[] res = new int[200];
    int[] targetIndex = new int[4000];
    List<Future<?>> futures = new ArrayList<>();

    Random random = new Random(System.currentTimeMillis());
    for (int i = 0; i < targetIndex.length; i++) {
      targetIndex[i] = random.nextInt(sharedData.length);
    }

    final int numberOfThreads = 32;
    final PageLockManager lockManager = new PageLockManager(32, 10, 32);
    ExecutorService incrementExecutors = Executors.newFixedThreadPool(numberOfThreads);
    ExecutorService decrementExecutors = Executors.newFixedThreadPool(numberOfThreads);
    ExecutorService tryIncrementExecutor = Executors.newFixedThreadPool(numberOfThreads);
    ExecutorService readExecutors = Executors.newFixedThreadPool(numberOfThreads);

    for (int i = 0; i < targetIndex.length; i++) {
      int finalI = i;
      Future<?> future =
          incrementExecutors.submit(
              () -> {
                int target = targetIndex[finalI];
                lockManager.lockWriteLock(target);
                try {
                  sharedData[target] += 1;
                  Thread.yield();
                } finally {
                  lockManager.unlockWriteLock(target);
                }
              });
      futures.add(future);
      future =
          decrementExecutors.submit(
              () -> {
                int target = targetIndex[targetIndex.length - 1 - finalI];
                lockManager.lockWriteLock(target);
                try {
                  sharedData[target] -= 1;
                  Thread.yield();
                } finally {
                  lockManager.unlockWriteLock(target);
                }
              });
      futures.add(future);
      future =
          readExecutors.submit(
              () -> {
                int target = targetIndex[finalI];
                lockManager.lockReadLock(target);
                try {
                  int j = sharedData[target];
                  Thread.yield();
                } finally {
                  lockManager.unlockReadLock(target);
                }
              });
      futures.add(future);
      futures.add(
          tryIncrementExecutor.submit(
              () -> {
                int target = targetIndex[finalI];
                if (lockManager.tryWriteLock(target)) {
                  try {
                    sharedData[target] += target;
                    res[target] += target;
                    Thread.yield();
                  } finally {
                    lockManager.unlockWriteLock(target);
                  }
                }
              }));
    }

    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        cause.printStackTrace();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    readExecutors.shutdown();
    incrementExecutors.shutdown();
    decrementExecutors.shutdown();
    readExecutors.awaitTermination(5, TimeUnit.SECONDS);
    incrementExecutors.awaitTermination(5, TimeUnit.SECONDS);
    decrementExecutors.awaitTermination(5, TimeUnit.SECONDS);

    for (int i = 0; i < sharedData.length; i++) {
      Assert.assertEquals(sharedData[i], res[i]);
    }
  }
}
