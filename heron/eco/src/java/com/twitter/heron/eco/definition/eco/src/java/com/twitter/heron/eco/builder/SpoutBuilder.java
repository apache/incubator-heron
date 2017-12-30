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
package com.twitter.heron.eco.definition.eco.src.java.com.twitter.heron.eco.builder;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.eco.definition.eco.src.java.com.twitter.heron.eco.definition.EcoExecutionContext;
import com.twitter.heron.eco.definition.eco.src.java.com.twitter.heron.eco.definition.EcoTopologyDefinition;
import com.twitter.heron.eco.definition.eco.src.java.com.twitter.heron.eco.definition.ObjectDefinition;


public class SpoutBuilder extends BaseBuilder {

  protected void addSpoutsToExecutionContext(EcoExecutionContext executionContext,
                                                    TopologyBuilder builder)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException,
      NoSuchFieldException, InvocationTargetException {
    EcoTopologyDefinition topologyDefinition = executionContext.getTopologyDefinition();
    Map<String, Object> spouts = new HashMap<>();

    for (ObjectDefinition def: topologyDefinition.getSpouts()) {
      Object obj = buildObject(def, executionContext);
      spouts.put(def.getId(), obj);
      builder.setSpout(def.getId(), (IRichSpout) obj, def.getParallelism());
    }
    executionContext.setSpouts(spouts);
  }
}
