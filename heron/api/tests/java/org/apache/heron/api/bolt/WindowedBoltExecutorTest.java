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

package org.apache.heron.api.bolt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.heron.api.Config;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.api.windowing.TupleWindow;
import org.apache.heron.api.windowing.WindowingConfigs;
import org.apache.heron.common.utils.topology.TopologyContextImpl;
import org.apache.heron.common.utils.tuple.TupleImpl;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link WindowedBoltExecutor}
 */
public class WindowedBoltExecutorTest {

  private WindowedBoltExecutor executor;
  private TestWindowedBolt testWindowedBolt;

  @SuppressWarnings("VisibilityModifier")
  private static class TestWindowedBolt extends BaseWindowedBolt {
    private static final long serialVersionUID = -8934326157586387333L;
    List<TupleWindow> tupleWindows = new ArrayList<>();

    @Override
    public void execute(TupleWindow input) {
      //System.out.println(input);
      tupleWindows.add(input);
    }
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
      public Fields getComponentOutputFields(String componentId, String streamId) {
        return fields;
      }

    };
  }

  private Tuple getTuple(String streamId, final Fields fields, Values values) {

    TopologyContext topologyContext = getContext(fields);
    return new TupleImpl(topologyContext, TopologyAPI.StreamId.newBuilder()
        .setId(streamId).setComponentName("s1")
        .build(), 0, null, values, 1) {
      @Override
      public TopologyAPI.StreamId getSourceGlobalStreamId() {
        return TopologyAPI.StreamId.newBuilder().setComponentName("s1").setId("default").build();
      }
    };
  }

  private OutputCollector getOutputCollector() {
    return Mockito.mock(OutputCollector.class);
  }

  private TopologyContext getTopologyContext() {
    TopologyContext context = Mockito.mock(TopologyContext.class);

    Map<TopologyAPI.StreamId, TopologyAPI.Grouping> sources =
        Collections.singletonMap(TopologyAPI.StreamId.newBuilder()
            .setComponentName("s1").setId("default").build(), null);
    Mockito.when(context.getThisSources()).thenReturn(sources);
    return context;
  }

  @Before
  public void setUp() {
    testWindowedBolt = new TestWindowedBolt();
    testWindowedBolt.withTimestampField("ts");
    executor = new WindowedBoltExecutor(testWindowedBolt);
    Map<String, Object> conf = new HashMap<>();
    conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 100000);
    conf.put(WindowingConfigs.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS, 20L);
    conf.put(WindowingConfigs.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS, 10L);
    conf.put(WindowingConfigs.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS, 5L);
    // trigger manually to avoid timing issues
    conf.put(WindowingConfigs.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS, 100000L);
    executor.prepare(conf, getTopologyContext(), getOutputCollector());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExecuteWithoutTs() throws Exception {
    executor.execute(getTuple("s1", new Fields("a"), new Values(1)));
  }

  @Test
  public void testExecuteWithTs() throws Exception {
    long[] timestamps = {603, 605, 607, 618, 626, 636};
    for (long ts : timestamps) {
      executor.execute(getTuple("s1", new Fields("ts"), new Values(ts)));
    }
    //Thread.sleep(120);
    executor.waterMarkEventGenerator.run();
    //System.out.println(testWindowedBolt.tupleWindows);
    assertEquals(3, testWindowedBolt.tupleWindows.size());
    TupleWindow first = testWindowedBolt.tupleWindows.get(0);
    assertArrayEquals(
        new long[]{603, 605, 607},
        new long[]{(long) first.get().get(0).getValue(0),
            (long) first.get().get(1).getValue(0),
            (long) first.get().get(2).getValue(0)});

    TupleWindow second = testWindowedBolt.tupleWindows.get(1);
    assertArrayEquals(
        new long[]{603, 605, 607, 618},
        new long[]{(long) second.get().get(0).getValue(0),
            (long) second.get().get(1).getValue(0),
            (long) second.get().get(2).getValue(0),
            (long) second.get().get(3).getValue(0)});

    TupleWindow third = testWindowedBolt.tupleWindows.get(2);
    assertArrayEquals(new long[]{618, 626}, new long[]{(long) third.get().get(0).getValue(0),
        (long) third.get().get(1).getValue(0)});
  }

  @Test
  public void testPrepareLateTupleStreamWithoutTs() throws Exception {
    Map<String, Object> conf = new HashMap<>();
    conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 100000);
    conf.put(WindowingConfigs.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS, 20L);
    conf.put(WindowingConfigs.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS, 10L);
    conf.put(WindowingConfigs.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM, "$late");
    conf.put(WindowingConfigs.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS, 5L);
    conf.put(WindowingConfigs.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS, 10L);

    testWindowedBolt = new TestWindowedBolt();
    executor = new WindowedBoltExecutor(testWindowedBolt);
    TopologyContext context = getTopologyContext();
    // emulate the call of withLateTupleStream method
    Mockito.when(context.getThisStreams()).thenReturn(new HashSet<>(Arrays.asList("default",
        "$late")));
    try {
      executor.prepare(conf, context, getOutputCollector());
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "Late tuple stream can be defined only when specifying a "
          + "timestamp field");
    }
  }

  @Test
  public void testPrepareLateTupleStreamWithoutBuilder() throws Exception {
    Map<String, Object> conf = new HashMap<>();
    conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 100000);
    conf.put(WindowingConfigs.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS, 20L);
    conf.put(WindowingConfigs.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS, 10L);
    conf.put(WindowingConfigs.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM, "$late");
    conf.put(WindowingConfigs.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS, 5L);
    conf.put(WindowingConfigs.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS, 10L);

    testWindowedBolt = new TestWindowedBolt();
    testWindowedBolt.withTimestampField("ts");
    executor = new WindowedBoltExecutor(testWindowedBolt);
    TopologyContext context = getTopologyContext();
    try {
      executor.prepare(conf, context, getOutputCollector());
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "Stream for late tuples must be defined with the builder "
          + "method withLateTupleStream");
    }
  }


  @Test
  public void testExecuteWithLateTupleStream() throws Exception {
    testWindowedBolt = new TestWindowedBolt();
    testWindowedBolt.withTimestampField("ts");
    executor = new WindowedBoltExecutor(testWindowedBolt);
    TopologyContext context = getTopologyContext();
    Mockito.when(context.getThisStreams()).thenReturn(new HashSet<>(Arrays.asList("default",
        "$late")));

    OutputCollector outputCollector = Mockito.mock(OutputCollector.class);
    Map<String, Object> conf = new HashMap<>();
    conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 100000);
    conf.put(WindowingConfigs.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS, 20L);
    conf.put(WindowingConfigs.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS, 10L);
    conf.put(WindowingConfigs.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM, "$late");
    conf.put(WindowingConfigs.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS, 5L);
    //Trigger manually to avoid timing issues
    conf.put(WindowingConfigs.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS, 1_000_000L);
    executor.prepare(conf, context, outputCollector);

    long[] timestamps = {603, 605, 607, 618, 626, 636, 600};
    List<Tuple> tuples = new ArrayList<>(timestamps.length);

    for (long ts : timestamps) {
      Tuple tuple = getTuple("s1", new Fields("ts"), new Values(ts));
      tuples.add(tuple);
      executor.execute(tuple);

      //Update the watermark to this timestamp
      executor.waterMarkEventGenerator.run();
    }
    System.out.println(testWindowedBolt.tupleWindows);
    Tuple tuple = tuples.get(tuples.size() - 1);
    Mockito.verify(outputCollector).emit("$late", Arrays.asList(tuple), new Values(tuple));
  }
}
