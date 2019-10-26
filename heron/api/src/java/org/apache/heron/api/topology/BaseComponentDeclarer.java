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

package org.apache.heron.api.topology;

import java.util.Map;

import com.google.protobuf.ByteString;

import org.apache.heron.api.Config;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.utils.Utils;

public abstract class BaseComponentDeclarer<T extends ComponentConfigurationDeclarer<?>>
    extends BaseConfigurationDeclarer<T> {
  private String name;
  private IComponent component;
  private Config componentConfiguration;

  public BaseComponentDeclarer(String name, IComponent comp, Number taskParallelism) {
    this.name = name;
    this.component = comp;
    if (comp.getComponentConfiguration() != null) {
      this.componentConfiguration = new Config(comp.getComponentConfiguration());
    } else {
      this.componentConfiguration = new Config();
    }
    if (taskParallelism != null) {
      Config.setComponentParallelism(this.componentConfiguration, taskParallelism.intValue());
    } else {
      Config.setComponentParallelism(this.componentConfiguration, 1);
    }
  }

  public abstract T returnThis();

  protected String getName() {
    return name;
  }

  @Override
  public T addConfigurations(Map<String, Object> conf) {
    componentConfiguration.putAll(conf);
    return returnThis();
  }

  public void dump(TopologyAPI.Component.Builder bldr) {
    bldr.setName(name);
    bldr.setSpec(TopologyAPI.ComponentObjectSpec.JAVA_SERIALIZED_OBJECT);
    bldr.setSerializedObject(ByteString.copyFrom(Utils.serialize(component)));
    bldr.setConfig(Utils.getConfigBuilder(componentConfiguration));
  }
}
