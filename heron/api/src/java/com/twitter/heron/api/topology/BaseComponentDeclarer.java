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

package com.twitter.heron.api.topology;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.utils.Utils;

public abstract class BaseComponentDeclarer<T extends ComponentConfigurationDeclarer<?>>
    extends BaseConfigurationDeclarer<T> {
  private static final Logger LOG = Logger.getLogger(BaseComponentDeclarer.class.getName());
  private String name;
  private IComponent component;
  private Map<String, Object> componentConfiguration;

  public BaseComponentDeclarer(String name, IComponent comp, Number taskParallelism) {
    this.name = name;
    this.component = comp;
    this.componentConfiguration = comp.getComponentConfiguration();
    if (this.componentConfiguration == null) {
      this.componentConfiguration = new HashMap<>();
    }
    if (taskParallelism != null) {
      this.componentConfiguration.put(Config.TOPOLOGY_COMPONENT_PARALLELISM,
          taskParallelism.toString());
    } else {
      this.componentConfiguration.put(Config.TOPOLOGY_COMPONENT_PARALLELISM,
          "1");
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

    TopologyAPI.Config.Builder cBldr = TopologyAPI.Config.newBuilder();
    for (Map.Entry<String, Object> entry : componentConfiguration.entrySet()) {
      if (entry.getKey() == null) {
        LOG.warning("ignore: config key is null");
        continue;
      }
      if (entry.getValue() == null) {
        LOG.warning("ignore: config key " + entry.getKey() + " has null value");
        continue;
      }
      TopologyAPI.Config.KeyValue.Builder kvBldr = TopologyAPI.Config.KeyValue.newBuilder();
      kvBldr.setKey(entry.getKey());
      kvBldr.setValue(entry.getValue().toString());
      kvBldr.setType(TopologyAPI.ConfigValueType.STRING_VALUE);
      cBldr.addKvs(kvBldr);
    }
    bldr.setConfig(cBldr);
  }
}
