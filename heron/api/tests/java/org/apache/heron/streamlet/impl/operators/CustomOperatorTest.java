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

import java.util.Arrays;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import org.apache.heron.api.Config;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.common.utils.topology.TopologyContextImpl;
import org.apache.heron.common.utils.tuple.TupleImpl;
import org.apache.heron.resource.TestCustomOperator;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CustomOperatorTest {
  private TestCustomOperator testOperator;

  private Tuple getTuple(String stream, final Fields fields, Values values) {
    TopologyAPI.StreamId streamId
        = TopologyAPI.StreamId.newBuilder()
        .setComponentName("custom1").setId(stream).build();
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

  @Before
  public void setUp() {
    testOperator = new TestCustomOperator<Integer>();
  }

  @Test
  public void testOutputTuples() {
    OutputCollector collector = PowerMockito.mock(OutputCollector.class);
    testOperator.prepare(new Config(), PowerMockito.mock(TopologyContext.class), collector);

    Tuple[] tuples = {
      getTuple("default", new Fields("a"), new Values(0)),    // to fail
      getTuple("default", new Fields("a"), new Values(1)),
      getTuple("default", new Fields("a"), new Values(2)),
      getTuple("default", new Fields("a"), new Values(3)),
      getTuple("default", new Fields("a"), new Values(4)),
      getTuple("default", new Fields("a"), new Values(5)),
      getTuple("default", new Fields("a"), new Values(101)),  // to skip
    };

    for (Tuple tuple: tuples) {
      testOperator.execute(tuple);
    }

    verify(collector, times(0)).emit(any(), eq(Arrays.asList(tuples[0])), any());
    verify(collector, times(0)).emit(any(), eq(Arrays.asList(tuples[6])), any());

    verify(collector, times(1)).emit("default", Arrays.asList(tuples[1]), Arrays.asList(1));
    verify(collector, times(1)).emit("default", Arrays.asList(tuples[2]), Arrays.asList(2));
    verify(collector, times(1)).emit("default", Arrays.asList(tuples[3]), Arrays.asList(3));
    verify(collector, times(1)).emit("default", Arrays.asList(tuples[4]), Arrays.asList(4));
    verify(collector, times(1)).emit("default", Arrays.asList(tuples[5]), Arrays.asList(5));
  }

  @Test
  public void testAckAndFail() {
    OutputCollector collector = PowerMockito.mock(OutputCollector.class);
    testOperator.prepare(new Config(), PowerMockito.mock(TopologyContext.class), collector);

    Tuple[] tuples = {
      getTuple("default", new Fields("a"), new Values(0)),    // tuple to fail
      getTuple("default", new Fields("a"), new Values(1)),    // tuples to process
      getTuple("default", new Fields("a"), new Values(101))   // tuple to skip but still ack
    };

    for (Tuple tuple: tuples) {
      testOperator.execute(tuple);
    }

    verify(collector, times(0)).ack(tuples[0]);
    verify(collector, times(1)).fail(tuples[0]);
    verify(collector, times(1)).ack(tuples[1]);
    verify(collector, times(0)).fail(tuples[1]);
    verify(collector, times(1)).ack(tuples[2]);
    verify(collector, times(0)).fail(tuples[2]);
  }
}
