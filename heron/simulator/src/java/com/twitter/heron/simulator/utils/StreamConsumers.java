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

package com.twitter.heron.simulator.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.HeronTuples;
import com.twitter.heron.simulator.grouping.Grouping;

/**
 * A stream could be consumed by many different downstream components by different grouping.
 * StreamConsumers is a class we could use to
 * fetch all task ids for all different components to send tuples to.
 */

public class StreamConsumers {
  private final List<Grouping> consumers = new ArrayList<>();

  public StreamConsumers() {

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
}
