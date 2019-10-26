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

package org.apache.heron.api.utils;

import java.util.ArrayList;

import org.junit.Test;

import org.apache.heron.api.serializer.KryoSerializer;

import junit.framework.TestCase;

public class UtilsTest extends TestCase {

  @Test
  public void testAssignKeyToTask() {
    ArrayList<Integer> taskIds = new ArrayList<Integer>();
    taskIds.add(1);
    taskIds.add(2);
    assertEquals(taskIds.size(), 2);
    assertEquals(Utils.assignKeyToTask(0, taskIds), Integer.valueOf(1));
    assertEquals(Utils.assignKeyToTask(100, taskIds), Integer.valueOf(1));
    assertEquals(Utils.assignKeyToTask(101, taskIds), Integer.valueOf(2));
    assertEquals(Utils.assignKeyToTask(-100, taskIds), Integer.valueOf(1));
    assertEquals(Utils.assignKeyToTask(-101, taskIds), Integer.valueOf(2));
  }

  @Test
  public void testnewInstance() {
    // Verify newInstance() works as expected to create a new instance of
    // org.apache.heron.api.serializer.KryoSerializer.
    Object o = Utils.newInstance("org.apache.heron.api.serializer.KryoSerializer");
    assertTrue(o instanceof KryoSerializer);
  }
}
