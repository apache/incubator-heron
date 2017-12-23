//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.eco.builder;

import java.util.HashMap;
import java.util.Map;

import com.twitter.heron.api.bolt.IBasicBolt;
import com.twitter.heron.api.bolt.IRichBolt;
import com.twitter.heron.api.bolt.IWindowedBolt;
import com.twitter.heron.api.grouping.CustomStreamGrouping;
import com.twitter.heron.api.topology.BoltDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.eco.definition.ComponentStream;
import com.twitter.heron.eco.definition.EcoExecutionContext;
import com.twitter.heron.eco.definition.EcoTopologyDefinition;
import com.twitter.heron.eco.definition.GroupingDefinition;
import com.twitter.heron.eco.definition.ObjectDefinition;
import com.twitter.heron.eco.definition.StreamDefinition;

public final class StreamBuilder extends BaseBuilder {

  private StreamBuilder() { }

  protected static void buildStreams(EcoExecutionContext executionContext, TopologyBuilder builder)
      throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    EcoTopologyDefinition topologyDefinition = executionContext.getTopologyDefinition();
    Map<String, ComponentStream> componentStreams = new HashMap<>();

    HashMap<String, BoltDeclarer> declarers = new HashMap<>();
    for (StreamDefinition stream : topologyDefinition.getStreams()) {
      Object boltObj = executionContext.getBolt(stream.getTo());
      BoltDeclarer declarer = declarers.get(stream.getTo());
      if (boltObj instanceof IRichBolt) {
        if (declarer == null) {
          declarer = builder.setBolt(stream.getTo(),
              (IRichBolt) boltObj,
              topologyDefinition.parallelismForBolt(stream.getTo()));
          declarers.put(stream.getTo(), declarer);
        }
      } else if (boltObj instanceof IBasicBolt) {
        if (declarer == null) {
          declarer = builder.setBolt(
              stream.getTo(),
              (IBasicBolt) boltObj,
              topologyDefinition.parallelismForBolt(stream.getTo()));
          declarers.put(stream.getTo(), declarer);
        }
      } else if (boltObj instanceof IWindowedBolt) {
        if (declarer == null) {
          declarer = builder.setBolt(
              stream.getTo(),
              (IWindowedBolt) boltObj,
              topologyDefinition.parallelismForBolt(stream.getTo()));
          declarers.put(stream.getTo(), declarer);
        }
      }  else {
        throw new IllegalArgumentException("Class does not appear to be a bolt: "
            + boltObj.getClass().getName());
      }

      GroupingDefinition grouping = stream.getGrouping();
      // if the streamId is defined, use it for the grouping,
      // otherwise assume default stream
      // Todo(joshfischer) Not sure if "default" is still valid
      String streamId = grouping.getStreamId() == null
          ? Utils.DEFAULT_STREAM_ID : grouping.getStreamId();


      switch (grouping.getType()) {
        case SHUFFLE:
          declarer.shuffleGrouping(stream.getFrom(), streamId);
          break;
        case FIELDS:
          //TODO check for null grouping args
          declarer.fieldsGrouping(stream.getFrom(), streamId, new Fields(grouping.getArgs()));
          break;
        case ALL:
          declarer.allGrouping(stream.getFrom(), streamId);
          break;
        case GLOBAL:
          declarer.globalGrouping(stream.getFrom(), streamId);
          break;
        case NONE:
          declarer.noneGrouping(stream.getFrom(), streamId);
          break;
        case CUSTOM:
          declarer.customGrouping(stream.getFrom(), streamId,
              buildCustomStreamGrouping(stream.getGrouping().getCustomClass()));
          break;
        default:
          throw new UnsupportedOperationException("unsupported grouping type: " + grouping);
      }
    }
    executionContext.setStreams(componentStreams);
  }

  private static CustomStreamGrouping buildCustomStreamGrouping(ObjectDefinition objectDefinition)
      throws ClassNotFoundException,
      IllegalAccessException, InstantiationException {
    Object grouping = buildObject(objectDefinition);
    return (CustomStreamGrouping) grouping;
  }
}
