//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.streamlet.impl.operators;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.bolt.IOutputCollector;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.windowing.TupleWindow;
import com.twitter.heron.api.windowing.TupleWindowImpl;
import com.twitter.heron.common.utils.topology.TopologyContextImpl;
import com.twitter.heron.common.utils.tuple.TupleImpl;
import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.Window;

public class ReduceByWindowOperatorTest {

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
    ReduceByWindowOperator<Integer> reduceOperator = getReduceByWindowOperator();

    TupleWindow tupleWindow = getTupleWindow();

    Integer expectedResult = 10;

    reduceOperator.execute(tupleWindow);

    Assert.assertEquals(1, emittedTuples.size());
    for (Object object : emittedTuples) {
      KeyValue<Window, Integer> tuple = (KeyValue<Window, Integer>) object;
      Window window = tuple.getKey();
      Assert.assertEquals(5, window.getCount());
      Assert.assertEquals(startTime, window.getStartTime());
      Assert.assertEquals(endTime, window.getEndTime());
      Assert.assertEquals(expectedResult, tuple.getValue());
    }
  }

  private TupleWindow getTupleWindow() {
    TopologyAPI.StreamId componentStreamId
        = TopologyAPI.StreamId.newBuilder()
        .setComponentName("sourceComponent").setId("default").build();

    List<Tuple> tuples = new LinkedList<>();
    for (int i = 0; i < 5; i++) {
      Tuple tuple = getTuple(componentStreamId, new Fields("a"),
            new Values(i));
      tuples.add(tuple);
    }

    TupleWindow tupleWindow = new TupleWindowImpl(tuples, new LinkedList<>(), new LinkedList<>(),
        startTime, endTime);
    return tupleWindow;
  }



  @SuppressWarnings({"rawtypes", "unchecked"})
  private ReduceByWindowOperator<Integer> getReduceByWindowOperator() {
    ReduceByWindowOperator<Integer> reduceByWindowOperator = new ReduceByWindowOperator<>(
        (o, o2) -> o + o2);

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
