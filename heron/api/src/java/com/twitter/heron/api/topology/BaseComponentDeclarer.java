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
import java.util.Set;
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

    // TODO: share this logic with HeronTopology.getConfigBuilder, which is identical. Differences
    // between the two have caused hard to troubleshoot bugs.
    Set<String> apiVars = Config.getApiVars();
    TopologyAPI.Config.Builder cBldr = TopologyAPI.Config.newBuilder();
    for (String key : componentConfiguration.keySet()) {
      Object value = componentConfiguration.get(key);
      if (key == null) {
        LOG.warning("ignore: config key is null");
        continue;
      }
      if (value == null) {
        LOG.warning("ignore: config key " + key + " has null value");
        continue;
      }
      TopologyAPI.Config.KeyValue.Builder kvBldr = TopologyAPI.Config.KeyValue.newBuilder();
      kvBldr.setKey(key);
      if (apiVars.contains(key)) {
        kvBldr.setType(TopologyAPI.ConfigValueType.STRING_VALUE);
        kvBldr.setValue(value.toString());
      } else {
        kvBldr.setType(TopologyAPI.ConfigValueType.JAVA_SERIALIZED_VALUE);
        kvBldr.setSerializedValue(ByteString.copyFrom(Utils.serialize(value)));
      }
      cBldr.addKvs(kvBldr);
    }
    bldr.setConfig(cBldr);
  }
}
