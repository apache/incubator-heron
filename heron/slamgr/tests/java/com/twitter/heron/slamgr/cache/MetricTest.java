//  Copyright 2016 Twitter. All rights reserved.
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
//  limitations under the License
package com.twitter.heron.slamgr.cache;


import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.slamgr.SLAMetrics;

public class MetricTest {
  @Test
  public void testMetric() {
    Metric m = new Metric("__emit-count", SLAMetrics.MetricAggregationType.SUM, 180 * 60, 60);

    m.AddValueToMetric("1");
    m.AddValueToMetric("2");
    m.AddValueToMetric("3");

    m.Purge();

    m.AddValueToMetric("4");
    m.AddValueToMetric("5");
    m.AddValueToMetric("6");

    //
    TopologyMaster.MetricResponse.IndividualMetric im = m.GetMetrics(false, 0, -1);

    Assert.assertEquals(im.getName(), "__emit-count");
    Assert.assertEquals(im.getValue(), "21");

    // minutely
    TopologyMaster.MetricResponse.IndividualMetric im2 = m.GetMetrics(true, 0, -1);

    Assert.assertEquals(im2.getIntervalValuesCount(), 2);
    Assert.assertEquals(im2.getIntervalValues(0).getValue(), "15");
    Assert.assertEquals(im2.getIntervalValues(1).getValue(), "6");
  }

}
