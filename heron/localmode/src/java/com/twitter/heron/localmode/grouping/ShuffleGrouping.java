package com.twitter.heron.localmode.grouping;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.twitter.heron.proto.system.HeronTuples;

public class ShuffleGrouping extends Grouping {
  private int nextTaskIndex;

  private final int taskIdsSize;

  public ShuffleGrouping(List<Integer> taskIds) {
    super(taskIds);

    taskIdsSize = taskIds.size();

    nextTaskIndex = new Random().nextInt(taskIds.size());
  }

  @Override
  public List<Integer> getListToSend(HeronTuples.HeronDataTuple tuple) {
    List<Integer> res = new ArrayList<>(1);
    res.add(taskIds.get(nextTaskIndex));
    nextTaskIndex = getNextTaskIndex(nextTaskIndex);

    return res;
  }

  private int getNextTaskIndex(int index) {
    index++;

    if (index == taskIdsSize) {
      index = 0;
    }

    return index;
  }
}
