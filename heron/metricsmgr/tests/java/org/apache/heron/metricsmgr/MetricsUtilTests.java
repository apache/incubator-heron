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

package org.apache.heron.metricsmgr;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.heron.spi.metricsmgr.metrics.MetricsRecord;

import static org.junit.Assert.assertEquals;

public class MetricsUtilTests {

  private final String host = "host";
  private final int port = 33;
  private final String component = "component";
  private final String instance = "instance";

  @Test
  public void testCreateSource() {
    final String expectedSource =
        String.format("%s:%d/%s/%s", host, port, component, instance);
    final String source = MetricsUtil.createSource(host, port, component, instance);

    assertEquals(expectedSource, source);
  }

  @Test
  public void testSplitRecordSource() {
    final String source = MetricsUtil.createSource(host, port, component, instance);
    MetricsRecord record = Mockito.mock(MetricsRecord.class);
    Mockito.when(record.getSource()).thenReturn(source);

    final String[] sourceComponents = MetricsUtil.splitRecordSource(record);

    assertEquals(3, sourceComponents.length);
    assertEquals(String.format("%s:%d", host, port), sourceComponents[0]);
    assertEquals(component, sourceComponents[1]);
    assertEquals(instance, sourceComponents[2]);
  }
}
