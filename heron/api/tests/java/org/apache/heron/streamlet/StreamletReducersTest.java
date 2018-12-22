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

package org.apache.heron.streamlet;

import org.junit.Assert;
import org.junit.Test;

public class StreamletReducersTest {

  @Test
  public void testSum() {
    Assert.assertEquals(StreamletReducers.sum(1, 2), (Integer) 3);
    Assert.assertEquals(StreamletReducers.sum(1L, 2L), (Long) 3L);
    Assert.assertEquals(StreamletReducers.sum(1.0f, 2.0f), (Float) 3.0f);
    Assert.assertEquals(StreamletReducers.sum(1.0, 2.0), (Double) 3.0);
  }

  @Test
  public void testMax() {
    Assert.assertEquals(StreamletReducers.max(1, 2), (Integer) 2);
    Assert.assertEquals(StreamletReducers.max(2, 1), (Integer) 2);
    Assert.assertEquals(StreamletReducers.max(1L, 2L), (Long) 2L);
    Assert.assertEquals(StreamletReducers.max(2L, 1L), (Long) 2L);
    Assert.assertEquals(StreamletReducers.max(1.0f, 2.0f), (Float) 2.0f);
    Assert.assertEquals(StreamletReducers.max(2.0f, 1.0f), (Float) 2.0f);
    Assert.assertEquals(StreamletReducers.max(1.0, 2.0), (Double) 2.0);
    Assert.assertEquals(StreamletReducers.max(2.0, 1.0), (Double) 2.0);
  }

  @Test
  public void testMin() {
    Assert.assertEquals(StreamletReducers.min(1, 2), (Integer) 1);
    Assert.assertEquals(StreamletReducers.min(2, 1), (Integer) 1);
    Assert.assertEquals(StreamletReducers.min(1L, 2L), (Long) 1L);
    Assert.assertEquals(StreamletReducers.min(2L, 1L), (Long) 1L);
    Assert.assertEquals(StreamletReducers.min(1.0f, 2.0f), (Float) 1.0f);
    Assert.assertEquals(StreamletReducers.min(2.0f, 1.0f), (Float) 1.0f);
    Assert.assertEquals(StreamletReducers.min(1.0, 2.0), (Double) 1.0);
    Assert.assertEquals(StreamletReducers.min(2.0, 1.0), (Double) 1.0);
  }
}
