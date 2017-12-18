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
package com.twitter.heron.eco.helper;

import java.io.InputStream;
import java.util.logging.Logger;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import com.twitter.heron.eco.definition.BoltDefinition;
import com.twitter.heron.eco.definition.EcoTopologyDefinition;
import com.twitter.heron.eco.definition.SpoutDefinition;

public final class EcoParser {
  private static final Logger LOG = Logger.getLogger(EcoParser.class.getName());

  private EcoParser() { }

  public static EcoTopologyDefinition parseFromInputStream(InputStream inputStream) {

    Yaml yaml = topologyYaml();

    if (inputStream == null) {
      LOG.info("Unable to load eco input stream");
    }

    return loadTopologyFromYaml(yaml, inputStream);

  }

  private static EcoTopologyDefinition loadTopologyFromYaml(Yaml yaml, InputStream inputStream) {
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
