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

public class MetricsRecordTest {
  private static final int N = 100;
  private static final String SOURCE = "source";
  private static final String CONTEXT = "ecoExecutionContext";
  private final List<MetricsRecord> records = new ArrayList<MetricsRecord>();

  @Before
  public void before() throws Exception {
    records.clear();
    for (int i = 0; i < N; i++) {
      MetricsRecord record =
          new MetricsRecord(i,
              SOURCE + i,
              new ArrayList<MetricsInfo>(),
              new ArrayList<ExceptionInfo>(),
              CONTEXT + i);
      records.add(record);
    }
  }

  @After
  public void after() throws Exception {
    records.clear();
  }

  /**
   * Method: getTimestamp()
   */
  @Test
  public void testGetTimestamp() throws Exception {
    for (int i = 0; i < N; i++) {
      Assert.assertTrue(records.get(i).getTimestamp() == i);
    }
  }

  /**
   * Method: getSource()
   */
  @Test
  public void testGetSource() throws Exception {
    for (int i = 0; i < N; i++) {
      Assert.assertTrue(records.get(i).getSource().equals(SOURCE + i));
    }
  }

  /**
   * Method: getMetrics()
   */
  @Test
  public void testGetMetrics() throws Exception {
    int items = 0;
    for (int i = 0; i < N; i++) {
      for (MetricsInfo info : records.get(i).getMetrics()) {
        items++;
      }
    }
    Assert.assertTrue(items == 0);
  }

  /**
   * Method: getExceptions()
   */
  @Test
  public void testGetExceptions() throws Exception {
    int items = 0;
    for (int i = 0; i < N; i++) {
      for (ExceptionInfo info : records.get(i).getExceptions()) {
        items++;
      }
    }
    Assert.assertTrue(items == 0);
  }

  /**
   * Method: getContext()
   */
  @Test
  public void testGetContext() throws Exception {
    for (int i = 0; i < N; i++) {
      Assert.assertTrue(records.get(i).getContext().equals(CONTEXT + i));
    }
  }
}
