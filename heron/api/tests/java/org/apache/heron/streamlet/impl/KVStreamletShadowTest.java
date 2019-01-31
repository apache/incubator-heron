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
package org.apache.heron.streamlet.impl.streamlets;

import org.junit.Test;
import org.mockito.Mock;

import org.apache.heron.streamlet.KeyValue;
import org.apache.heron.streamlet.impl.StreamletImpl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link StreamletShadow}
 */
public class KVStreamletShadowTest {
  @Mock
  private StreamletImpl<KeyValue<String, Double>> mockReal = mock(StreamletImpl.class);

  @Mock
  private StreamletImpl<Double> mockChild = mock(StreamletImpl.class);

  @Test
  public void testConstruction() {
    doReturn("real_name").when(mockReal).getName();
    doReturn(1).when(mockReal).getNumPartitions();
    doNothing().when(mockReal).addChild(mockChild);
    doReturn(mockReal).when(mockReal).setName("shadow_name");
    doReturn(mockReal).when(mockReal).setNumPartitions(2);
    doReturn("real_stream").when(mockReal).getStreamId();

    KVStreamletShadow<String, Double> shadow = new KVStreamletShadow(mockReal);
    assertEquals(shadow.getName(), "real_name");
    assertEquals(shadow.getNumPartitions(), 1);

    // Set name/partition should be applied to the real object
    KVStreamletShadow<String, Double> shadow2 = new KVStreamletShadow(mockReal)
        .setName("shadow_name")
        .setNumPartitions(2);

    verify(mockReal, times(1)).setName("shadow_name");
    verify(mockReal, times(1)).setNumPartitions(2);

    // addChild call should be forwarded to the real object
    verify(mockReal, never()).addChild(mockChild);
    shadow.addChild(mockChild);
    shadow2.addChild(mockChild);
    verify(mockReal, times(2)).addChild(mockChild);
  }
}
