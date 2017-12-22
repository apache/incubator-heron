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
import java.util.logging.Logger;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.bolt.IBasicBolt;
import com.twitter.heron.api.bolt.IRichBolt;
import com.twitter.heron.api.bolt.IWindowedBolt;
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

public final class EcoBuilder {
  private static final Logger LOG = Logger.getLogger(EcoBuilder.class.getName());

  private EcoBuilder() { }

  public static TopologyBuilder buildTopologyBuilder(EcoExecutionContext executionContext)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {

    buildSpouts(executionContext);
    buildBolts(executionContext);
    TopologyBuilder builder = new TopologyBuilder();
    buildStreams(executionContext, builder);

    return builder;
  }


  private static void buildStreams(EcoExecutionContext executionContext, TopologyBuilder builder) {
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

//      BoltDefinition boltDef = topologyDefinition.getBoltDef(stream.getTo());
//      if (boltDef.getOnHeapMemoryLoad() > -1) {
//        if (boltDef.getOffHeapMemoryLoad() > -1) {
//          declarer.setMemoryLoad(boltDef.getOnHeapMemoryLoad(), boltDef.getOffHeapMemoryLoad());
//        } else {
//          declarer.setMemoryLoad(boltDef.getOnHeapMemoryLoad());
//        }
//      }
//      if (boltDef.getCpuLoad() > -1) {
//        declarer.setCPULoad(boltDef.getCpuLoad());
//      }
//      if (boltDef.getNumTasks() > -1) {
//        declarer.setNumTasks(boltDef.getNumTasks());
//      }

      GroupingDefinition grouping = stream.getGrouping();
      // if the streamId is defined, use it for the grouping,
      // otherwise assume storm's default stream
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
//        case CUSTOM:
//          declarer.customGrouping(stream.getFrom(), streamId,
//              buildCustomStreamGrouping(stream.getGrouping().getCustomClass(), context));
//          break;
        default:
          throw new UnsupportedOperationException("unsupported grouping type: " + grouping);
      }
    }

    executionContext.setStreams(componentStreams);

  }

  private static void buildBolts(EcoExecutionContext executionContext)
      throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    EcoTopologyDefinition topologyDefinition = executionContext.getTopologyDefinition();
    Map<String, Object> bolts = new HashMap<>();

    for (ObjectDefinition def: topologyDefinition.getBolts()) {
      Object obj = buildObject(def);
      bolts.put(def.getId(), obj);
    }

    executionContext.setBolts(bolts);
  }

  private static void buildSpouts(EcoExecutionContext executionContext)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    EcoTopologyDefinition topologyDefinition = executionContext.getTopologyDefinition();
    Map<String, Object> spouts = new HashMap<>();

    for (ObjectDefinition def: topologyDefinition.getSpouts()) {
      Object obj = buildObject(def);
      spouts.put(def.getId(), obj);
    }

    executionContext.setSpouts(spouts);

  }

  @SuppressWarnings("rawtypes")
  private static Object buildObject(ObjectDefinition def)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    Class clazz = Class.forName(def.getClassName());
    return clazz.newInstance();

  }

  public static Config buildConfig(EcoTopologyDefinition topologyDefinition) {
    Map<String, Object> configMap = topologyDefinition.getConfig();
    if (configMap == null) {
      return new Config();
    } else {
      Config config = new Config();
      for (Map.Entry<String, Object> entry: topologyDefinition.getConfig().entrySet()) {
        config.put(entry.getKey(), entry.getValue());
      }
      return config;

    }
  }

}
