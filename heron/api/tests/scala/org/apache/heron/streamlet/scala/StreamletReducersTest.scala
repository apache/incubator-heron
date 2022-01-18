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
package org.apache.heron.streamlet.scala

import org.junit.Assert.assertEquals

import org.apache.heron.streamlet.scala.common.BaseFunSuite

class StreamletReducersTest extends BaseFunSuite {

  test("Sum should work correctly") {
    assertEquals(StreamletReducers.sum(1, 2), 3)
    assertEquals(StreamletReducers.sum(1L, 2L), 3L)
    assertEquals(StreamletReducers.sum(1.0f, 2.0f), 3.0f, 0.01f)
    assertEquals(StreamletReducers.sum(1.0, 2.0), 3.0, 0.01)
  }

  test("Max should work correctly") {
    assertEquals(StreamletReducers.max(1, 2), 2)
    assertEquals(StreamletReducers.max(2, 1), 2)
    assertEquals(StreamletReducers.max(1L, 2L), 2L)
    assertEquals(StreamletReducers.max(2L, 1L), 2L)
    assertEquals(StreamletReducers.max(1.0f, 2.0f), 2.0f, 0.01f)
    assertEquals(StreamletReducers.max(2.0f, 1.0f), 2.0f, 0.01f)
    assertEquals(StreamletReducers.max(1.0, 2.0), 2.0, 0.01)
    assertEquals(StreamletReducers.max(2.0, 1.0), 2.0, 0.01)
  }

  test("Min should work correctly") {
    assertEquals(StreamletReducers.min(1, 2), 1)
    assertEquals(StreamletReducers.min(2, 1), 1)
    assertEquals(StreamletReducers.min(1L, 2L), 1L)
    assertEquals(StreamletReducers.min(2L, 1L), 1L)
    assertEquals(StreamletReducers.min(1.0f, 2.0f), 1.0f, 0.01f)
    assertEquals(StreamletReducers.min(2.0f, 1.0f), 1.0f, 0.01f)
    assertEquals(StreamletReducers.min(1.0, 2.0), 1.0, 0.01)
    assertEquals(StreamletReducers.min(2.0, 1.0), 1.0, 0.01)
  }
}
