package com.twitter.heron.localmode.grouping;

import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.proto.system.HeronTuples;

import junit.framework.Assert;

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
    List<Integer> task_ids = new LinkedList<>();
    for (int i = 0; i < 100; ++i) {
      task_ids.add(i);
    }

    LowestGrouping g = new LowestGrouping(task_ids);
    for (int i = 0; i < 1000; ++i) {
      HeronTuples.HeronDataTuple dummy = HeronTuples.HeronDataTuple.getDefaultInstance();
      List<Integer> dest = g.getListToSend(dummy);

      Assert.assertEquals(dest.size(), 1);
      Assert.assertEquals(dest.get(0), (Integer) 0);
    }
  }
}
