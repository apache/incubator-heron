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

package com.twitter.heron.metrics;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.metric.GlobalMetrics;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.common.basics.WakeableLooper;
import com.twitter.heron.common.utils.metrics.MetricsCollector;
import com.twitter.heron.common.utils.topology.TopologyContextImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for CounterFactory
 */
public class GlobalMetricsTest {
  @Test
  public void testGlobalMetrics() {
    MetricsCollector fakeCollector = new MetricsCollector(new FakeWakeableLooper(), null);
    TopologyContext fakeContext = new TopologyContextImpl(new HashMap<String, Object>(),
        TopologyAPI.Topology.getDefaultInstance(),
        new HashMap<Integer, String>(),
        0,
        fakeCollector);
    GlobalMetrics.init(fakeContext, 5);
    GlobalMetrics.incr("mycounter");
    Map<String, Long> metricsContent = GlobalMetrics.getUnderlyingCounter().getValueAndReset();
    assertTrue(metricsContent.containsKey("mycounter"));
    assertEquals(1, metricsContent.size());
    assertEquals(new Long(1), metricsContent.get("mycounter"));

    // Increment two different counters
    GlobalMetrics.incr("mycounter1");
    GlobalMetrics.incr("mycounter2");
    GlobalMetrics.incr("mycounter1");
    metricsContent = GlobalMetrics.getUnderlyingCounter().getValueAndReset();
    assertTrue(metricsContent.containsKey("mycounter"));
    assertTrue(metricsContent.containsKey("mycounter1"));
    assertTrue(metricsContent.containsKey("mycounter2"));
    assertEquals(3L, metricsContent.size());
    assertEquals(new Long(0), metricsContent.get("mycounter"));
    assertEquals(new Long(1), metricsContent.get("mycounter2"));
    assertEquals(new Long(2), metricsContent.get("mycounter1"));
  }

  // TODO: Use JMock framework for mock. (Needs extra jar)
  private static class FakeWakeableLooper extends WakeableLooper {
    protected void doWait() {
    }

    public void wakeUp() {
    }
  }
}
