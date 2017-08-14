// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.storm.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.api.generated.TopologyAPI;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link WindowedBoltExecutor}
 */
public class WindowedBoltExecutorTest {

  private WindowedBoltExecutor executor;
  private TestWindowedBolt testWindowedBolt;

  @SuppressWarnings("serial")
  private static class TestWindowedBolt extends BaseWindowedBolt {
    private List<TupleWindow> tupleWindows = new ArrayList<>();

    @Override
    public void execute(TupleWindow input) {
      tupleWindows.add(input);
    }
  }

  private Tuple getTuple(String streamId, final Fields fields, Values values) {
    com.twitter.heron.api.topology.TopologyContext context =
        Mockito.mock(com.twitter.heron.common.utils.topology.TopologyContextImpl.class);
    com.twitter.heron.api.tuple.Fields f = new com.twitter.heron.api.tuple.Fields(fields.toList());
    Mockito.when(
        context.getComponentOutputFields(Mockito.anyString(), Mockito.anyString())).thenReturn(f);
    TopologyAPI.StreamId stream =
        TopologyAPI.StreamId.newBuilder().setId(streamId).setComponentName("c1").build();
    return new TupleImpl(new com.twitter.heron.common.utils.tuple.TupleImpl(
        context, stream, 1, null, values));
  }

  private OutputCollector getOutputCollector() {
    return Mockito.mock(OutputCollector.class);
  }

  private TopologyContext getTopologyContext() {
    TopologyContext context = Mockito.mock(TopologyContext.class);
    Set<GlobalStreamId> sources = Collections.singleton(new GlobalStreamId("s1", "default"));
    Mockito.when(context.getThisSourceIds()).thenReturn(sources);
    return context;
  }

  @Before
  public void setUp() {
    testWindowedBolt = new TestWindowedBolt();
    executor = new WindowedBoltExecutor(testWindowedBolt);
    Map<String, Object> conf = new HashMap<>();
    conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 100000);
    conf.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS, 20);
    conf.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS, 10);
    conf.put(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_FIELD_NAME, "ts");
    conf.put(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS, 5);
    // trigger manually to avoid timing issues
    conf.put(Config.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS, 100000);
    executor.prepare(conf, getTopologyContext(), getOutputCollector());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExecuteWithoutTs() throws Exception {
    executor.execute(getTuple("s1", new Fields("a"), new Values(1)));
  }

  @Test
  public void testExecuteWithTs() throws Exception {
    long[] timstamps = {603, 605, 607, 618, 626, 636};
    for (long ts : timstamps) {
      executor.execute(getTuple("s1", new Fields("ts"), new Values(ts)));
    }
    //Thread.sleep(120);
    executor.waterMarkEventGenerator.run();
    assertEquals(3, testWindowedBolt.tupleWindows.size());
    TupleWindow first = testWindowedBolt.tupleWindows.get(0);
    assertArrayEquals(new long[]{603, 605, 607},
        new long[]{(long) first.get().get(0).getValue(0), (long) first.get().get(1).getValue(0),
            (long) first.get().get(2).getValue(0)});

    TupleWindow second = testWindowedBolt.tupleWindows.get(1);
    assertArrayEquals(new long[]{603, 605, 607, 618},
        new long[]{(long) second.get().get(0).getValue(0), (long) second.get().get(1).getValue(0),
            (long) second.get().get(2).getValue(0), (long) second.get().get(3).getValue(0)});

    TupleWindow third = testWindowedBolt.tupleWindows.get(2);
    assertArrayEquals(new long[]{618, 626},
        new long[]{(long) third.get().get(0).getValue(0), (long) third.get().get(1).getValue(0)});
  }
}
