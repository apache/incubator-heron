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

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import com.twitter.heron.eco.definition.EcoExecutionContext;
import com.twitter.heron.eco.definition.EcoTopologyDefinition;
import com.twitter.heron.eco.definition.ObjectDefinition;

public class BoltBuilder extends BaseBuilder {

  protected void buildBolts(EcoExecutionContext executionContext)
      throws IllegalAccessException, InstantiationException, ClassNotFoundException,
      NoSuchFieldException, InvocationTargetException {
    EcoTopologyDefinition topologyDefinition = executionContext.getTopologyDefinition();
    Map<String, Object> bolts = new HashMap<>();

    for (ObjectDefinition def: topologyDefinition.getBolts()) {
      Object obj = buildObject(def, executionContext);
      bolts.put(def.getId(), obj);
    }
    executionContext.setBolts(bolts);
  }
}
