/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.eco.builder.heron;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.heron.api.bolt.IBasicBolt;
import org.apache.heron.api.bolt.IRichBolt;
import org.apache.heron.api.bolt.IStatefulWindowedBolt;
import org.apache.heron.api.bolt.IWindowedBolt;
import org.apache.heron.api.grouping.CustomStreamGrouping;
import org.apache.heron.api.topology.BoltDeclarer;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.utils.Utils;
import org.apache.heron.eco.builder.ObjectBuilder;
import org.apache.heron.eco.definition.ComponentStream;
import org.apache.heron.eco.definition.EcoExecutionContext;
import org.apache.heron.eco.definition.EcoTopologyDefinition;
import org.apache.heron.eco.definition.GroupingDefinition;
import org.apache.heron.eco.definition.ObjectDefinition;
import org.apache.heron.eco.definition.StreamDefinition;

public class StreamBuilder {

  protected <K extends Serializable, V extends Serializable> void buildStreams(
      EcoExecutionContext executionContext, TopologyBuilder builder, ObjectBuilder objectBuilder)
      throws IllegalAccessException, InstantiationException, ClassNotFoundException,
      NoSuchFieldException, InvocationTargetException {
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
      } else if (boltObj instanceof IStatefulWindowedBolt) {
        if (declarer == null) {
          //noinspection unchecked
          declarer = builder.setBolt(
              stream.getTo(),
              (IStatefulWindowedBolt<K, V>) boltObj,
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
      String streamId = grouping.getStreamId() == null
          ? Utils.DEFAULT_STREAM_ID : grouping.getStreamId();


      switch (grouping.getType()) {
        case SHUFFLE:
          declarer.shuffleGrouping(stream.getFrom(), streamId);
          break;
        case FIELDS:
          List<String> groupingArgs = grouping.getArgs();
          if (groupingArgs == null) {
            throw new IllegalArgumentException("You must supply arguments for Fields grouping");
          }
          declarer.fieldsGrouping(stream.getFrom(), streamId, new Fields(groupingArgs));
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
              buildCustomStreamGrouping(stream.getGrouping().getCustomClass(),
                  executionContext,
                  objectBuilder));
          break;
        default:
          throw new UnsupportedOperationException("unsupported grouping type: " + grouping);
      }
    }
    executionContext.setStreams(componentStreams);
  }

  private CustomStreamGrouping buildCustomStreamGrouping(ObjectDefinition objectDefinition,
                                                         EcoExecutionContext executionContext,
                                                         ObjectBuilder objectBuilder)
      throws ClassNotFoundException,
      IllegalAccessException, InstantiationException, NoSuchFieldException,
      InvocationTargetException {
    Object grouping = objectBuilder.buildObject(objectDefinition, executionContext);
    return (CustomStreamGrouping) grouping;
  }
}
