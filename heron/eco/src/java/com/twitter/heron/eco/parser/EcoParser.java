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
package com.twitter.heron.eco.parser;

import java.io.InputStream;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import com.twitter.heron.eco.definition.BoltDefinition;
import com.twitter.heron.eco.definition.EcoTopologyDefinition;
import com.twitter.heron.eco.definition.SpoutDefinition;

public class EcoParser {

  public EcoTopologyDefinition parseFromInputStream(InputStream inputStream)
      throws Exception {

    Yaml yaml = topologyYaml();

    if (inputStream == null) {
      throw new Exception("Unable to load eco input stream");
    }
    return loadTopologyFromYaml(yaml, inputStream);
  }

  private EcoTopologyDefinition loadTopologyFromYaml(Yaml yaml, InputStream inputStream) {
    return (EcoTopologyDefinition) yaml.load(inputStream);
  }
  private static Yaml topologyYaml() {
    Constructor topologyConstructor = new Constructor(EcoTopologyDefinition.class);

    TypeDescription topologyDescription = new TypeDescription(EcoTopologyDefinition.class);

    topologyDescription.putListPropertyType("spouts", SpoutDefinition.class);
    topologyDescription.putListPropertyType("bolts", BoltDefinition.class);
    topologyConstructor.addTypeDescription(topologyDescription);

    return new Yaml(topologyConstructor);
  }
}
