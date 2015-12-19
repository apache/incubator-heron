package com.twitter.heron.localmode.grouping;

import java.util.List;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.HeronTuples;

public abstract class Grouping {
  protected List<Integer> taskIds;

  public Grouping(List<Integer> taskIds) {
    this.taskIds = taskIds;
  }

  public abstract List<Integer> getListToSend(HeronTuples.HeronDataTuple tuple);

  public static Grouping create(TopologyAPI.Grouping grouping,
                                TopologyAPI.InputStream inputStream,
                                TopologyAPI.StreamSchema schema,
                                List<Integer> taskIds) {
    switch (grouping) {
      case SHUFFLE: {
        return new ShuffleGrouping(taskIds);
      }

      case FIELDS: {
        return new FieldsGrouping(inputStream, schema, taskIds);
      }

      case ALL: {
        return new AllGrouping(taskIds);
      }

      case LOWEST: {
        return new LowestGrouping(taskIds);
      }

      case NONE: {
        // This is what we are doing in production right now
        return new ShuffleGrouping(taskIds);
      }

      case CUSTOM: {
        return new CustomGrouping(taskIds);
      }

      case DIRECT: {
        throw new IllegalArgumentException("Direct Grouping not supported");
      }

      default: {
        throw new IllegalArgumentException("Unknown Grouping");
      }
    }
  }
}
