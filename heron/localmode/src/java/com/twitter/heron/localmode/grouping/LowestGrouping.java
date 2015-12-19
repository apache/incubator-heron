package com.twitter.heron.localmode.grouping;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.twitter.heron.proto.system.HeronTuples;

public class LowestGrouping extends Grouping {
  private int lowestTaskId;

  public LowestGrouping(List<Integer> taskIds) {
    super(taskIds);

    lowestTaskId = Collections.min(taskIds);
  }

  @Override
  public List<Integer> getListToSend(HeronTuples.HeronDataTuple tuple) {
    List<Integer> res = new LinkedList<>();
    res.add(lowestTaskId);

    return res;
  }
}
