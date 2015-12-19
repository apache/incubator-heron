package com.twitter.heron.localmode.grouping;

import java.util.LinkedList;
import java.util.List;

import com.twitter.heron.proto.system.HeronTuples;

public class AllGrouping extends Grouping {
  public AllGrouping(List<Integer> taskIds) {
    super(taskIds);
  }

  @Override
  public List<Integer> getListToSend(HeronTuples.HeronDataTuple tuple) {
    return new LinkedList<>(taskIds);
  }
}
