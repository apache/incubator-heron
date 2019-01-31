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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
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
import org.apache.heron.streamlet.KeyValue;
import org.apache.heron.streamlet.KeyedWindow;

public class GeneralReduceByKeyAndWindowOperatorTest {

  private List<Object> emittedTuples;
  private long startTime = 1508099660801L;
  private long endTime = startTime + 1000L;

  @Before
  public void setUp() {
    emittedTuples = new LinkedList<>();
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testReduceByWindowOperator() {
    GeneralReduceByKeyAndWindowOperator<KeyValue<String, Integer>, String, Integer> reduceOperator =
        getReduceByWindowOperator(12);

    TupleWindow tupleWindow = getTupleWindow(3, 5);

    HashMap<String, Integer> expectedResults = new HashMap<>();
    expectedResults.put("0", 22);
    expectedResults.put("1", 22);
    expectedResults.put("2", 22);

    reduceOperator.execute(tupleWindow);

    Assert.assertEquals(3, emittedTuples.size());
    for (Object object : emittedTuples) {
      KeyValue<KeyedWindow<String>, Integer> tuple =
          (KeyValue<KeyedWindow<String>, Integer>) object;
      KeyedWindow<String> window = tuple.getKey();
      String key = window.getKey();
      Assert.assertEquals(5, window.getWindow().getCount());
      Assert.assertEquals(startTime, window.getWindow().getStartTime());
      Assert.assertEquals(endTime, window.getWindow().getEndTime());
      Assert.assertEquals(expectedResults.get(key), tuple.getValue());
    }
  }

  private TupleWindow getTupleWindow(int nkeys, int count) {
    TopologyAPI.StreamId componentStreamId
        = TopologyAPI.StreamId.newBuilder()
        .setComponentName("sourceComponent").setId("default").build();

    List<Tuple> tuples = new LinkedList<>();
    for (int i = 0; i < nkeys; i++) {
      for (int j = 0; j < count; ++j) {
        Tuple tuple = getTuple(componentStreamId, new Fields("a"),
            new Values(new KeyValue<>(String.valueOf(i), j)));
        tuples.add(tuple);
      }
    }

    TupleWindow tupleWindow = new TupleWindowImpl(tuples, new LinkedList<>(), new LinkedList<>(),
        startTime, endTime);
    return tupleWindow;
  }



  @SuppressWarnings({"rawtypes", "unchecked"})
  private GeneralReduceByKeyAndWindowOperator<KeyValue<String, Integer>, String, Integer>
        getReduceByWindowOperator(Integer identity) {
    GeneralReduceByKeyAndWindowOperator<KeyValue<String, Integer>, String, Integer>
        reduceByWindowOperator = new GeneralReduceByKeyAndWindowOperator<>(
            x -> x.getKey(), identity, (o, o2) -> o + o2.getValue());

    reduceByWindowOperator.prepare(new Config(), PowerMockito.mock(TopologyContext.class),
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
    return reduceByWindowOperator;
  }

  private Tuple getTuple(TopologyAPI.StreamId streamId, final Fields fields, Values values) {

    TopologyContext topologyContext = getContext(fields);
    return new TupleImpl(topologyContext, streamId, 0,
        null, values, 1) {
      @Override
      public TopologyAPI.StreamId getSourceGlobalStreamId() {
        return TopologyAPI.StreamId.newBuilder().setComponentName("sourceComponent")
            .setId("default").build();
      }
    };
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
