package com.twitter.heron.localmode.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.localmode.grouping.Grouping;
import com.twitter.heron.proto.system.HeronTuples;

/**
 * A stream could be consumed by many different downstream components by different grouping.
 * StreamConsumers is a class we could use to
 * fetch all task ids for all different components to send tuples to.
 */

public class StreamConsumers {
  private final List<Grouping> consumers = new ArrayList<>();

  public StreamConsumers() {

  }

  public void newConsumer(TopologyAPI.InputStream inputStream,
                          TopologyAPI.StreamSchema schema,
                          List<Integer> taskIds) {
    consumers.add(Grouping.create(inputStream.getGtype(), inputStream, schema, taskIds));
  }

  /**
   * Get all task ids from different components to send for a data tuple
   *
   * @param tuple the tuple to send
   * @return the target task ids to send tuple to
   */
  public List<Integer> getListToSend(HeronTuples.HeronDataTuple tuple) {
    List<Integer> res = new ArrayList<>();

    for (Grouping consumer : consumers) {
      res.addAll(consumer.getListToSend(tuple));
    }

    return res;
  }

  // For unit test
  protected List<Grouping> getConsumers() {
    return consumers;
  }

  /**
   * Populate the Stream Consumers for the whole topology given the topology protobuf.
   *
   * @param topology The given topology protobuf
   * @param componentToTaskIds the map of componentName to its list of taskIds in the topology
   * @return the populated stream consumers' map
   */
  public static Map<TopologyAPI.StreamId, StreamConsumers> populateStreamConsumers(
      TopologyAPI.Topology topology,
      Map<String, List<Integer>> componentToTaskIds
  ) {
    // First get a map of (TopologyAPI.StreamId -> TopologyAPI.StreamSchema)
    Map<TopologyAPI.StreamId, TopologyAPI.StreamSchema> streamToSchema =
        new HashMap<>();

    // Calculate spout's output stream
    for (TopologyAPI.Spout spout : topology.getSpoutsList()) {
      for (TopologyAPI.OutputStream outputStream : spout.getOutputsList()) {
        streamToSchema.put(outputStream.getStream(), outputStream.getSchema());
      }
    }

    // Calculate bolt's output stream
    for (TopologyAPI.Bolt bolt : topology.getBoltsList()) {
      for (TopologyAPI.OutputStream outputStream : bolt.getOutputsList()) {
        streamToSchema.put(outputStream.getStream(), outputStream.getSchema());
      }
    }

    // Then we would calculate the populated map of stream consumers
    Map<TopologyAPI.StreamId, StreamConsumers> populatedStreamConsumers =
        new HashMap<>();
    // Only bolts could consume from input stream
    for (TopologyAPI.Bolt bolt : topology.getBoltsList()) {
      for (TopologyAPI.InputStream inputStream : bolt.getInputsList()) {
        TopologyAPI.StreamSchema schema = streamToSchema.get(inputStream.getStream());

        String componentName = bolt.getComp().getName();

        List<Integer> taskIds = componentToTaskIds.get(componentName);

        // Add component's task ids into stream consumers
        if (!populatedStreamConsumers.containsKey(inputStream.getStream())) {
          populatedStreamConsumers.put(inputStream.getStream(), new StreamConsumers());
        }

        populatedStreamConsumers.
            get(inputStream.getStream()).
            newConsumer(inputStream, schema, taskIds);
      }
    }

    return populatedStreamConsumers;
  }
}
