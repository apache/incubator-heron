/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.simulator.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.proto.system.HeronTuples;

/**
 * TupleCache Tester.
 */
public class TupleCacheTest {
  public static final int N = 10;
  public static final int REPEAT = 10;

  private static final int SRC_TASK_ID = 1;
  private static List<Integer> destTaskIds;
  private static HeronTuples.HeronDataTuple dataTuple;
  private static HeronTuples.AckTuple ackTuple;
  private static TopologyAPI.StreamId stream0;
  private static TopologyAPI.StreamId stream1;

  private TupleCache tupleCache;


  @BeforeClass
  public static void beforeClass() throws Exception {
    destTaskIds = new ArrayList<>();
    for (int i = 0; i < N; i++) {
      destTaskIds.add(i);
    }

    dataTuple = HeronTuples.HeronDataTuple.getDefaultInstance();
    ackTuple = HeronTuples.AckTuple.newBuilder().setAckedtuple(0).build();

    stream0 = TopologyAPI.StreamId.newBuilder().
        setComponentName("0").
        setId("0").
        build();

    stream1 = TopologyAPI.StreamId.newBuilder().
        setComponentName("1").
        setId("1").
        build();
  }

  @Before
  public void before() throws Exception {
    tupleCache = new TupleCache();
  }

  @After
  public void after() throws Exception {
    tupleCache.clear();
  }

  /**
   * Method: addDataTuple(int destTaskId, TopologyAPI.StreamId streamId, HeronTuples.HeronDataTuple tuple)
   */
  @Test
  public void testAddDataTupleBatch() throws Exception {


    // Test for Batch should apply

    for (int i = 0; i < N * REPEAT; i++) {
      tupleCache.addDataTuple(SRC_TASK_ID,
          destTaskIds.get(i % destTaskIds.size()), stream0, dataTuple, true);
    }

    for (int i = 0; i < N * REPEAT; i++) {
      tupleCache.addDataTuple(SRC_TASK_ID,
          destTaskIds.get(i % destTaskIds.size()), stream1, dataTuple, true);
    }

    Map<Integer, List<HeronTuples.HeronTupleSet>> cache = tupleCache.getCache();
    Assert.assertEquals(N, cache.size());
    Assert.assertEquals(new HashSet<>(destTaskIds), cache.keySet());
    for (List<HeronTuples.HeronTupleSet> tuples : cache.values()) {
      Assert.assertEquals(2, tuples.size());
      for (HeronTuples.HeronTupleSet tupleSet : tuples) {
        Assert.assertEquals(REPEAT, tupleSet.getData().getTuplesCount());
        for (HeronTuples.HeronDataTuple tuple : tupleSet.getData().getTuplesList()) {
          Assert.assertNotNull(tuple.getKey());
        }
      }
    }
  }

  /**
   * Method: addDataTuple(int destTaskId, TopologyAPI.StreamId streamId, HeronTuples.HeronDataTuple tuple)
   */
  @Test
  public void testAddDataTupleNoBatch() throws Exception {
    // Test for Batch should not apply
    for (int i = 0; i < N * REPEAT; i++) {
      tupleCache.addDataTuple(SRC_TASK_ID,
          destTaskIds.get(i % destTaskIds.size()), stream0, dataTuple, true);
      tupleCache.addDataTuple(SRC_TASK_ID,
          destTaskIds.get(i % destTaskIds.size()), stream1, dataTuple, true);
    }

    Map<Integer, List<HeronTuples.HeronTupleSet>> cache = tupleCache.getCache();
    Assert.assertEquals(N, cache.size());
    Assert.assertEquals(new HashSet<>(destTaskIds), cache.keySet());
    for (List<HeronTuples.HeronTupleSet> tuples : cache.values()) {
      Assert.assertEquals(2 * REPEAT, tuples.size());
      for (HeronTuples.HeronTupleSet tupleSet : tuples) {
        Assert.assertEquals(1, tupleSet.getData().getTuplesCount());
        for (HeronTuples.HeronDataTuple tuple : tupleSet.getData().getTuplesList()) {
          Assert.assertNotNull(tuple.getKey());
        }
      }
    }
  }

  /**
   * Method: addAckTuple(int taskId, HeronTuples.AckTuple tuple)
   */
  @Test
  public void testAddAckTupleBatch() throws Exception {


    // Test for Batch should apply
    for (int i = 0; i < N * REPEAT; i++) {
      tupleCache.addAckTuple(SRC_TASK_ID,
          destTaskIds.get(i % destTaskIds.size()), ackTuple);
      tupleCache.addFailTuple(SRC_TASK_ID,
          destTaskIds.get(i % destTaskIds.size()), ackTuple);
      tupleCache.addEmitTuple(SRC_TASK_ID,
          destTaskIds.get(i % destTaskIds.size()), ackTuple);
    }

    Map<Integer, List<HeronTuples.HeronTupleSet>> cache = tupleCache.getCache();
    cache = tupleCache.getCache();

    Assert.assertEquals(N, cache.size());
    Assert.assertEquals(new HashSet<>(destTaskIds), cache.keySet());
    for (List<HeronTuples.HeronTupleSet> tuples : cache.values()) {
      Assert.assertEquals(3 * REPEAT, tuples.size());
    }
  }

  /**
   * Method: addAckTuple(int taskId, HeronTuples.AckTuple tuple)
   */
  @Test
  public void testAddAckTupleNoBatch() throws Exception {
    for (int i = 0; i < N * REPEAT; i++) {
      tupleCache.addAckTuple(SRC_TASK_ID,
          destTaskIds.get(i % destTaskIds.size()), ackTuple);
    }

    for (int i = 0; i < N * REPEAT; i++) {
      tupleCache.addFailTuple(SRC_TASK_ID,
          destTaskIds.get(i % destTaskIds.size()), ackTuple);
    }

    for (int i = 0; i < N * REPEAT; i++) {
      tupleCache.addEmitTuple(SRC_TASK_ID,
          destTaskIds.get(i % destTaskIds.size()), ackTuple);
    }

    Map<Integer, List<HeronTuples.HeronTupleSet>> cache = tupleCache.getCache();
    Assert.assertEquals(N, cache.size());
    Assert.assertEquals(new HashSet<>(destTaskIds), cache.keySet());
    for (List<HeronTuples.HeronTupleSet> tuples : cache.values()) {
      Assert.assertEquals(3, tuples.size());
    }
  }
}
