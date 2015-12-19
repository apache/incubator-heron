package com.twitter.heron.localmode.grouping;

import java.util.LinkedList;
import java.util.List;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.HeronTuples;

public class FieldsGrouping extends Grouping {
  private final List<Integer> fieldsGroupingIndices = new LinkedList<>();

  public FieldsGrouping(TopologyAPI.InputStream inputStream,
                        TopologyAPI.StreamSchema schema,
                        List<Integer> taskIds) {
    super(taskIds);

    for (int i = 0; i < schema.getKeysCount(); i++) {
      for (int j = 0; i < inputStream.getGroupingFields().getKeysCount(); j++) {
        if (schema.getKeys(i).getKey().equals(inputStream.getGroupingFields().getKeys(j).getKey())) {
          fieldsGroupingIndices.add(i);
          break;
        }
      }
    }
  }

  @Override
  public List<Integer> getListToSend(HeronTuples.HeronDataTuple tuple) {
    List<Integer> res = new LinkedList<>();

    int taskIndex = 0;
    int primeNumber = 633910111;
    for (Integer indices : fieldsGroupingIndices) {
      taskIndex += Math.abs(tuple.getValues(indices).hashCode()) % primeNumber;
    }

    taskIndex = taskIndex % taskIds.size();
    res.add(taskIds.get(taskIndex));

    return res;
  }
}
