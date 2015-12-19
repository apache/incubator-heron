package com.twitter.heron.localmode.grouping;

import java.util.LinkedList;
import java.util.List;

import com.twitter.heron.proto.system.HeronTuples;

public class CustomGrouping extends Grouping {
  public CustomGrouping(List<Integer> taskIds) {
    super(taskIds);
  }

  @Override
  public List<Integer> getListToSend(HeronTuples.HeronDataTuple tuple) {
    // Stmgr does not do the custom grouping.
    // That is done by the instance
    return new LinkedList<>();
  }
}
