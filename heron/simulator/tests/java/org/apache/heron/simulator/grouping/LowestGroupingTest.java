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

package org.apache.heron.simulator.grouping;

import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.heron.proto.system.HeronTuples;

public class LowestGroupingTest {

  @Before
  public void before() throws Exception {
  }

  @After
  public void after() throws Exception {
  }

  /**
   * Method: getListToSend(HeronTuples.HeronDataTuple tuple)
   */
  @Test
  public void testGetListToSend() throws Exception {
    List<Integer> taskIds = new LinkedList<>();
    for (int i = 0; i < 100; ++i) {
      taskIds.add(i);
    }

    LowestGrouping g = new LowestGrouping(taskIds);
    for (int i = 0; i < 1000; ++i) {
      HeronTuples.HeronDataTuple dummy = HeronTuples.HeronDataTuple.getDefaultInstance();
      List<Integer> dest = g.getListToSend(dummy);

      Assert.assertEquals(dest.size(), 1);
      Assert.assertEquals(dest.get(0), (Integer) 0);
    }
  }
}
