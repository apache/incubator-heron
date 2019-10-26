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

package org.apache.heron.metrics;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.heron.api.metric.MultiAssignableMetric;

/**
 * Test for MultiAssignableMetric
 */
public class MultiAssignableMetricTest {
  @Test
  public void testMultiAssignableMetrics() {
    MultiAssignableMetric<Integer> multiAssignableMetric = new MultiAssignableMetric<>(0);
    multiAssignableMetric.scope("metric_a").setValue(100);
    multiAssignableMetric.scope("metric_b").setValue(200);

    Map<String, Integer> ret = multiAssignableMetric.getValueAndReset();
    Assert.assertEquals(ret.get("metric_a"), new Integer(100));
    Assert.assertEquals(ret.get("metric_b"), new Integer(200));

    // Re-assign by synchronized safeScope
    multiAssignableMetric.safeScope("metric_a").setValue(300);
    multiAssignableMetric.safeScope("metric_b").setValue(400);

    ret = multiAssignableMetric.getValueAndReset();
    Assert.assertEquals(ret.get("metric_a"), new Integer(300));
    Assert.assertEquals(ret.get("metric_b"), new Integer(400));
  }


}
