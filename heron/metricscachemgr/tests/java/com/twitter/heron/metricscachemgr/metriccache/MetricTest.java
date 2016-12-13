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
package com.twitter.heron.metricscachemgr.metriccache;


import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.metricscachemgr.SLAMetrics;

public class MetricTest {
  private static String debugFilePath = "/tmp/" + MetricTest.class.getSimpleName() + ".debug.txt";

  private Path file = null;
  private List<String> lines = null;

  @Before
  public void before() {
    file = Paths.get(debugFilePath);
    lines = new ArrayList<>();
  }

  @After
  public void after() throws IOException {
    Files.write(file, lines, Charset.forName("UTF-8"));
  }

  @Test
  public void testMetric() {
    lines.add("testMetric");
    Metric m = new Metric("__emit-count", SLAMetrics.MetricAggregationType.SUM, 10, 60);

    m.AddValueToMetric("1");
    m.AddValueToMetric("2");
    m.AddValueToMetric("3");

    m.Purge();

    m.AddValueToMetric("4");
    m.AddValueToMetric("5");
    m.AddValueToMetric("6");

    lines.add(m.toString());

    //
    TopologyMaster.MetricResponse.IndividualMetric im = m.GetMetrics(false, 0, -1);

    Assert.assertEquals(im.getName(), "__emit-count");
    Assert.assertEquals(im.getValue(), "21.0");
  }


  @Test
  public void testMetricMinutely() {
    lines.add("testMetricMinutely");
    Metric m = new Metric("__emit-count", SLAMetrics.MetricAggregationType.SUM, 10, 60);

    m.AddValueToMetric("1");
    m.AddValueToMetric("2");
    m.AddValueToMetric("3");

    m.Purge();

    m.AddValueToMetric("4");
    m.AddValueToMetric("5");
    m.AddValueToMetric("6");

    lines.add(m.toString());

    // minutely
    TopologyMaster.MetricResponse.IndividualMetric im2 = m.GetMetrics(true, 0, Integer.MAX_VALUE);

    Assert.assertEquals(im2.getIntervalValuesCount(), 10);
    Assert.assertEquals(im2.getIntervalValues(0).getValue(), "15.0");
    Assert.assertEquals(im2.getIntervalValues(1).getValue(), "6.0");
    Assert.assertEquals(im2.getIntervalValues(2).getValue(), "0.0");
  }

}
