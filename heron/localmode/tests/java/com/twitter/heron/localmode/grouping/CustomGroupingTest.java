package com.twitter.heron.localmode.grouping;

import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.proto.system.HeronTuples;

import junit.framework.Assert;

public class CustomGroupingTest {

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
    task_ids.add(0);
    task_ids.add(2);
    task_ids.add(4);
    task_ids.add(8);

    CustomGrouping g = new CustomGrouping(task_ids);
    HeronTuples.HeronDataTuple tuple = HeronTuples.HeronDataTuple.getDefaultInstance();

    for (int i = 0; i < 1000; ++i) {
      List<Integer> dests = g.getListToSend(tuple);

      Assert.assertEquals(dests.size(), 0);
    }
  }
}
