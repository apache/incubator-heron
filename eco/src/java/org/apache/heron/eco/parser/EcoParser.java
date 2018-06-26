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

package org.apache.heron.eco.parser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import org.apache.heron.eco.definition.BoltDefinition;
import org.apache.heron.eco.definition.EcoTopologyDefinition;
import org.apache.heron.eco.definition.SpoutDefinition;

public class EcoParser {

  private static final Logger LOG = Logger.getLogger(EcoParser.class.getName());

  public EcoTopologyDefinition parseFromInputStream(InputStream inputStream,
                                                    InputStream propsFile, boolean envFilter)
      throws Exception {

    Yaml yaml = topologyYaml();

    if (inputStream == null) {
      throw new Exception("Unable to load eco input stream");
    }
    return loadTopologyFromYaml(yaml, inputStream, propsFile, envFilter);
  }

  private EcoTopologyDefinition loadTopologyFromYaml(Yaml yaml, InputStream inputStream,
                                                     InputStream propsIn,
                                                     boolean envFilter) throws IOException {
    LOG.info("Parsing eco config file");
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    int b;
    while ((b = inputStream.read()) != -1) {
      bos.write(b);
    }

    String yamlDefinitionStr = bos.toString();
    // properties file substitution
    if (propsIn != null) {
      LOG.info("Performing property substitution.");
      Properties props = new Properties();
      props.load(propsIn);
      for (Object key : props.keySet()) {
        yamlDefinitionStr =
            yamlDefinitionStr.replace("${" + key + "}", props.getProperty((String) key));
      }
    } else {
      LOG.info("Not performing property substitution.");
    }

    // environment variable substitution
    if (envFilter) {
      LOG.info("Performing environment variable substitution.");
      Map<String, String> envs = System.getenv();
      for (String key : envs.keySet()) {
        yamlDefinitionStr = yamlDefinitionStr.replace("${ENV-" + key + "}", envs.get(key));
      }
    } else {
      LOG.info("Not performing environment variable substitution.");
    }
    return (EcoTopologyDefinition) yaml.load(yamlDefinitionStr);
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
