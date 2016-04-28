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

package com.twitter.heron.localmode.grouping;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.proto.system.HeronTuples;

import org.junit.Assert;

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
    List<Integer> task_ids = new LinkedList<>();
    for (int i = 0; i < 100; ++i) {
      task_ids.add(i);
    }

    AllGrouping g = new AllGrouping(task_ids);
    for (int i = 0; i < 1000; ++i) {
      HeronTuples.HeronDataTuple dummy = HeronTuples.HeronDataTuple.getDefaultInstance();
      List<Integer> dest = g.getListToSend(dummy);
      Assert.assertEquals(dest.size(), task_ids.size());
      Collections.sort(dest);
      for (int j = 0; j < task_ids.size(); ++j) {
        Assert.assertEquals(task_ids.get(j), dest.get(j));
      }
    }
  }
} 
