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

package org.apache.heron.streamlet.impl.operators;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import org.apache.heron.api.Config;
import org.apache.heron.api.bolt.IOutputCollector;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.api.windowing.TupleWindow;
import org.apache.heron.api.windowing.TupleWindowImpl;
import org.apache.heron.common.utils.topology.TopologyContextImpl;
import org.apache.heron.common.utils.tuple.TupleImpl;
import org.apache.heron.streamlet.JoinType;
import org.apache.heron.streamlet.KeyValue;
import org.apache.heron.streamlet.KeyedWindow;
import org.apache.heron.streamlet.SerializableBiFunction;
import org.apache.heron.streamlet.SerializableFunction;

public class JoinOperatorTest {

  private List<Object> emittedTuples;
  private long startTime = 1508099660801L;
  private long endTime = startTime + 1000L;

  @Before
  public void setUp() {
    emittedTuples = new LinkedList<>();
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testInnerJoinOperator() {
    JoinOperator<String, KeyValue<String, String>, KeyValue<String, String>, String> joinOperator
        = getJoinOperator(JoinType.INNER);

    TupleWindow tupleWindow = getTupleWindow();

    Set<String> expectedResults = new HashSet<>();
    expectedResults.add("01");
    expectedResults.add("03");
    expectedResults.add("21");
    expectedResults.add("23");
    expectedResults.add("41");
    expectedResults.add("43");

    joinOperator.execute(tupleWindow);

    Assert.assertEquals(2 * 3, emittedTuples.size());
    for (Object object : emittedTuples) {
      KeyValue<KeyedWindow<String>, String> tuple = (KeyValue<KeyedWindow<String>, String>) object;
      KeyedWindow<String> keyedWindow = tuple.getKey();
      Assert.assertEquals("key1", keyedWindow.getKey());
      Assert.assertEquals(12, keyedWindow.getWindow().getCount());
      Assert.assertEquals(startTime, keyedWindow.getWindow().getStartTime());
      Assert.assertEquals(endTime, keyedWindow.getWindow().getEndTime());
      Assert.assertTrue(expectedResults.contains(tuple.getValue()));
      expectedResults.remove(tuple.getValue());
    }
    Assert.assertEquals(0, expectedResults.size());
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testOuterLeftJoinOperator() {
    JoinOperator<String, KeyValue<String, String>, KeyValue<String, String>, String> joinOperator
        = getJoinOperator(JoinType.OUTER_LEFT);

    TupleWindow tupleWindow = getTupleWindow();

    Set<String> expectedResultsK1 = new HashSet<>();
    expectedResultsK1.add("01");
    expectedResultsK1.add("03");
    expectedResultsK1.add("21");
    expectedResultsK1.add("23");
    expectedResultsK1.add("41");
    expectedResultsK1.add("43");

    Set<String> expectedResultsK2 = new HashSet<>();
    expectedResultsK2.add("5null");
    expectedResultsK2.add("6null");
    expectedResultsK2.add("7null");


    joinOperator.execute(tupleWindow);

    Assert.assertEquals(9, emittedTuples.size());
    for (Object object : emittedTuples) {
      KeyValue<KeyedWindow<String>, String> tuple = (KeyValue<KeyedWindow<String>, String>) object;
      KeyedWindow<String> keyedWindow = tuple.getKey();
      switch (keyedWindow.getKey()) {
        case "key1":
          Assert.assertTrue(expectedResultsK1.contains(tuple.getValue()));
          expectedResultsK1.remove(tuple.getValue());
          break;
        case "key2":
          Assert.assertTrue(expectedResultsK2.contains(tuple.getValue()));
          expectedResultsK2.remove(tuple.getValue());
          break;
        default:
          Assert.fail();
      }
      Assert.assertEquals(12, keyedWindow.getWindow().getCount());
      Assert.assertEquals(startTime, keyedWindow.getWindow().getStartTime());
      Assert.assertEquals(endTime, keyedWindow.getWindow().getEndTime());
    }
    Assert.assertEquals(0, expectedResultsK1.size());
    Assert.assertEquals(0, expectedResultsK2.size());
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testOuterRightJoinOperator() {
    JoinOperator<String, KeyValue<String, String>, KeyValue<String, String>, String> joinOperator
        = getJoinOperator(JoinType.OUTER_RIGHT);

    TupleWindow tupleWindow = getTupleWindow();

    Set<String> expectedResultsK1 = new HashSet<>();
    expectedResultsK1.add("01");
    expectedResultsK1.add("03");
    expectedResultsK1.add("21");
    expectedResultsK1.add("23");
    expectedResultsK1.add("41");
    expectedResultsK1.add("43");

    Set<String> expectedResultsK2 = new HashSet<>();
    expectedResultsK2.add("null8");
    expectedResultsK2.add("null9");
    expectedResultsK2.add("null10");
    expectedResultsK2.add("null11");

    joinOperator.execute(tupleWindow);

    Assert.assertEquals(10, emittedTuples.size());
    for (Object object : emittedTuples) {
      KeyValue<KeyedWindow<String>, String> tuple = (KeyValue<KeyedWindow<String>, String>) object;
      KeyedWindow<String> keyedWindow = tuple.getKey();
      switch (keyedWindow.getKey()) {
        case "key1":
          Assert.assertTrue(expectedResultsK1.contains(tuple.getValue()));
          expectedResultsK1.remove(tuple.getValue());
          break;
        case "key2":
          Assert.assertTrue(expectedResultsK2.contains(tuple.getValue()));
          expectedResultsK2.remove(tuple.getValue());
          break;
        case "key3":
          Assert.assertTrue(expectedResultsK2.contains(tuple.getValue()));
          expectedResultsK2.remove(tuple.getValue());
          break;
        default:
          Assert.fail();
      }
      Assert.assertEquals(12, keyedWindow.getWindow().getCount());
      Assert.assertEquals(startTime, keyedWindow.getWindow().getStartTime());
      Assert.assertEquals(endTime, keyedWindow.getWindow().getEndTime());
    }
    Assert.assertEquals(0, expectedResultsK1.size());
    Assert.assertEquals(0, expectedResultsK2.size());
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testOuterJoinOperator() {
    JoinOperator<String, KeyValue<String, String>, KeyValue<String, String>, String> joinOperator
        = getJoinOperator(JoinType.OUTER);

    TupleWindow tupleWindow = getTupleWindow();

    Set<String> expectedResultsK1 = new HashSet<>();
    expectedResultsK1.add("01");
    expectedResultsK1.add("03");
    expectedResultsK1.add("21");
    expectedResultsK1.add("23");
    expectedResultsK1.add("41");
    expectedResultsK1.add("43");

    Set<String> expectedResultsK2 = new HashSet<>();
    expectedResultsK2.add("5null");
    expectedResultsK2.add("6null");
    expectedResultsK2.add("7null");

    Set<String> expectedResultsK3 = new HashSet<>();
    expectedResultsK3.add("null8");
    expectedResultsK3.add("null9");
    expectedResultsK3.add("null10");
    expectedResultsK3.add("null11");

    joinOperator.execute(tupleWindow);

    Assert.assertEquals(13, emittedTuples.size());
    for (Object object : emittedTuples) {
      KeyValue<KeyedWindow<String>, String> tuple = (KeyValue<KeyedWindow<String>, String>) object;
      KeyedWindow<String> keyedWindow = tuple.getKey();
      switch (keyedWindow.getKey()) {
        case "key1":
          Assert.assertTrue(expectedResultsK1.contains(tuple.getValue()));
          expectedResultsK1.remove(tuple.getValue());
          break;
        case "key2":
          Assert.assertTrue(expectedResultsK2.contains(tuple.getValue()));
          expectedResultsK2.remove(tuple.getValue());
          break;
        case "key3":
          Assert.assertTrue(expectedResultsK3.contains(tuple.getValue()));
          expectedResultsK3.remove(tuple.getValue());
          break;
        default:
          Assert.fail();
      }
      Assert.assertEquals(12, keyedWindow.getWindow().getCount());
      Assert.assertEquals(startTime, keyedWindow.getWindow().getStartTime());
      Assert.assertEquals(endTime, keyedWindow.getWindow().getEndTime());
    }
    Assert.assertEquals(0, expectedResultsK1.size());
    Assert.assertEquals(0, expectedResultsK3.size());
  }

  private TupleWindow getTupleWindow() {
    TopologyAPI.StreamId leftComponentStreamId
        = TopologyAPI.StreamId.newBuilder()
        .setComponentName("leftComponent").setId("s1").build();

    TopologyAPI.StreamId rightComponentStreamId
        = TopologyAPI.StreamId.newBuilder()
        .setComponentName("rightComponent").setId("s1").build();

    List<Tuple> tuples = new LinkedList<>();
    for (int i = 0; i < 5; i++) {
      Tuple tuple;
      if (i % 2 == 0) {
        tuple = getTuple(leftComponentStreamId, new Fields("a"),
            new Values(new KeyValue<String, String>("key1", String.valueOf(i))));
      } else {
        tuple = getTuple(rightComponentStreamId, new Fields("a"),
            new Values(new KeyValue<String, String>("key1", String.valueOf(i))));
      }
      tuples.add(tuple);
    }

    for (int i = 5; i < 8; i++) {
      Tuple tuple = getTuple(leftComponentStreamId, new Fields("a"),
          new Values(new KeyValue<String, String>("key2", String.valueOf(i))));
      tuples.add(tuple);
    }

    for (int i = 8; i < 12; i++) {
      Tuple tuple = getTuple(rightComponentStreamId, new Fields("a"),
          new Values(new KeyValue<String, String>("key3", String.valueOf(i))));
      tuples.add(tuple);
    }


    TupleWindow tupleWindow = new TupleWindowImpl(tuples, new LinkedList<>(), new LinkedList<>(),
        startTime, endTime);
    return tupleWindow;
  }



  @SuppressWarnings({"rawtypes", "unchecked"})
  private JoinOperator<String, KeyValue<String, String>, KeyValue<String, String>, String>
        getJoinOperator(JoinType type) {
    SerializableFunction<KeyValue<String, String>, String> f = x -> x == null ? "null" : x.getKey();
    JoinOperator<String, KeyValue<String, String>, KeyValue<String, String>, String> joinOperator =
        new JoinOperator(
        type,
        "leftComponent",
        "rightComponent",
            f,
            f,
        (SerializableBiFunction<KeyValue<String, String>, KeyValue<String, String>, String>)
            (o, o2) -> (o == null ? "null" : o.getValue()) + (o2 == null ? "null" : o2.getValue()));

    joinOperator.prepare(new Config(), PowerMockito.mock(TopologyContext.class),
        new OutputCollector(new IOutputCollector() {

          @Override
          public void reportError(Throwable error) {

          }

          @Override
          public List<Integer> emit(String streamId,
                                    Collection<Tuple> anchors, List<Object> tuple) {
            emittedTuples.addAll(tuple);
            return null;
          }

          @Override
          public void emitDirect(int taskId, String streamId,
                                 Collection<Tuple> anchors, List<Object> tuple) {

          }

          @Override
          public void ack(Tuple input) {

          }

          @Override
          public void fail(Tuple input) {

          }
        }));
    return joinOperator;
  }

  private Tuple getTuple(TopologyAPI.StreamId streamId, final Fields fields, Values values) {

    TopologyContext topologyContext = getContext(fields);
    return new TupleImpl(topologyContext, streamId, 0,
        null, values, 1) {
      @Override
      public TopologyAPI.StreamId getSourceGlobalStreamId() {
        return TopologyAPI.StreamId.newBuilder().setComponentName("s1").setId("default").build();
      }
    };
  }

  private TopologyContext getTopologyContext() {
    TopologyContext context = Mockito.mock(TopologyContext.class);

    Map<TopologyAPI.StreamId, TopologyAPI.Grouping> sources =
        Collections.singletonMap(TopologyAPI.StreamId.newBuilder()
            .setComponentName("s1").setId("default").build(), null);
    Mockito.when(context.getThisSources()).thenReturn(sources);
    return context;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private TopologyContext getContext(final Fields fields) {
    TopologyBuilder builder = new TopologyBuilder();
    return new TopologyContextImpl(new Config(),
        builder.createTopology()
            .setConfig(new Config())
            .setName("test")
            .setState(TopologyAPI.TopologyState.RUNNING)
            .getTopology(),
        new HashMap(), 1, null) {
      @Override
      public Fields getComponentOutputFields(
          String componentId, String streamId) {
        return fields;
      }

    };
  }
}
