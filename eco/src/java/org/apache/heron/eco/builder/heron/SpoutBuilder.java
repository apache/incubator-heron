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

import java.lang.reflect.InvocationTargetException;

import org.apache.heron.api.spout.IRichSpout;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.eco.builder.ObjectBuilder;
import org.apache.heron.eco.definition.EcoExecutionContext;
import org.apache.heron.eco.definition.EcoTopologyDefinition;
import org.apache.heron.eco.definition.ObjectDefinition;

public class SpoutBuilder {

  protected void buildSpouts(EcoExecutionContext executionContext,
                             TopologyBuilder builder,
                             ObjectBuilder objectBuilder)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException,
      NoSuchFieldException, InvocationTargetException {
    EcoTopologyDefinition topologyDefinition = executionContext.getTopologyDefinition();

    for (ObjectDefinition def: topologyDefinition.getSpouts()) {
      Object obj = objectBuilder.buildObject(def, executionContext);
      builder.setSpout(def.getId(), (IRichSpout) obj, def.getParallelism());
      executionContext.addSpout(def.getId(), obj);
    }
  }
}
