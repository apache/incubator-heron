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

import java.util.ArrayList;

import org.junit.Test;
import org.mockito.Mock;

import org.apache.heron.streamlet.impl.StreamletImpl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link StreamletShadow}
 */
public class StreamletShadowTest {
  @Mock
  private StreamletImpl<Double> mockReal;

  @Mock
  private StreamletImpl<Double> mockChild;

  @Test
  public void testConstruction() {
    doReturn("real_name").when(mockReal).getName();
    doReturn(1).when(mockReal).getNumPartitions();
    doNothing().when(mockReal).addChild(mockChild);
    doReturn(new ArrayList<StreamletImpl<Double>>()).when(mockReal).getChildren();
    doReturn("real_stream").when(mockReal).getStreamId();

    StreamletShadow<Double> shadow = new StreamletShadow(mockReal);
    assertEquals(shadow.getName(), "real_name");
    assertEquals(shadow.getNumPartitions(), 1);
    assertEquals(shadow.getStreamId(), "real_stream");

    // set a different stream id
    shadow.setStreamId("shadow_stream");
    assertEquals(shadow.getStreamId(), "shadow_stream");

    // addChild call should be forwarded to the real object
    verify(mockReal, never()).addChild(mockChild);
    shadow.addChild(mockChild);
    verify(mockReal, times(1)).addChild(mockChild);
  }

}
