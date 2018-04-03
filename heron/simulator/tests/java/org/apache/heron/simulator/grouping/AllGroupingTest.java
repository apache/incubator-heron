// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.heron.simulator.grouping;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.heron.proto.system.HeronTuples.HeronDataTuple;

import static org.junit.Assert.assertEquals;

/**
 * AllGrouping Tester.
 */
public class AllGroupingTest {

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

    AllGrouping g = new AllGrouping(taskIds);
    for (int i = 0; i < 1000; ++i) {
      HeronDataTuple dummy = HeronDataTuple.getDefaultInstance();
      List<Integer> dest = g.getListToSend(dummy);
      assertEquals(dest.size(), taskIds.size());
      Collections.sort(dest);
      for (int j = 0; j < taskIds.size(); ++j) {
        assertEquals(taskIds.get(j), dest.get(j));
      }
    }
  }
}