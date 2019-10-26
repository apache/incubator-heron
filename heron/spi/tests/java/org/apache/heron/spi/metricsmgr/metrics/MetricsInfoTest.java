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

package org.apache.heron.spi.metricsmgr.metrics;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MetricsInfoTest {
  private static final int N = 100;
  private static final String NAME = "name";
  private static final String VALUE = "value";
  private final List<MetricsInfo> metricsInfos = new ArrayList<MetricsInfo>();

  @Before
  public void before() throws Exception {
    metricsInfos.clear();
    for (int i = 0; i < N; i++) {
      MetricsInfo metricsInfo = new MetricsInfo(NAME + i, VALUE + i);
      metricsInfos.add(metricsInfo);
    }
  }

  @After
  public void after() throws Exception {
    metricsInfos.clear();
  }

  /**
   * Method: getName()
   */
  @Test
  public void testGetName() throws Exception {
    for (int i = 0; i < N; i++) {
      Assert.assertTrue(metricsInfos.get(i).getName().equals(NAME + i));
    }
  }

  /**
   * Method: getValue()
   */
  @Test
  public void testGetValue() throws Exception {
    for (int i = 0; i < N; i++) {
      Assert.assertTrue(metricsInfos.get(i).getValue().equals(VALUE + i));
    }
  }
}
