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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import com.twitter.heron.eco.definition.EcoTopologyDefinition;
import com.twitter.heron.eco.definition.ComponentResourceDefinition;
import com.twitter.heron.eco.definition.ResourceDefinition;

import sun.misc.Resource;

@SuppressWarnings("unchecked")
public class ConfigBuilder {

  private static final String COMPONENT_RESOURCE_MAP = "topology.component.resourcemap";
  private static final String ID = "id";
  private static final String RAM = "ram";
  private static final String CPU = "cpu";
  private static final String DISK = "disk";
  private static final String EQUALS = "=";
  protected Config buildConfig(EcoTopologyDefinition topologyDefinition) throws Exception {

    Map<String, Object> configMap = topologyDefinition.getConfig();
    Config config = new Config();
    for (Map.Entry<String, Object> entry: configMap.entrySet()) {

      if (entry.getKey().equals(COMPONENT_RESOURCE_MAP)) {
        ComponentResourceDefinition componentResourceDefinition = new ComponentResourceDefinition();

          List<Object> objects = (List<Object>) entry.getValue();
          for (Object obj: objects) {

            String objString = obj.toString();

            int idIndex = objString.indexOf(ID);
            int equalsIndex;
            int spaceIndex;

            // Do this for each value
            if (idIndex != -1) {
              equalsIndex = objString.indexOf(EQUALS, idIndex);
              spaceIndex = objString.indexOf(",", idIndex);
              String idValue = objString.substring(equalsIndex + 1 , spaceIndex);
              System.out.println("ID VALUE IS: " + idValue);

            }

            int ramIndex = objString.indexOf(RAM);
            int cpuIndex = objString.indexOf(CPU);
            int diskIndex = objString.indexOf(DISK);


            System.out.println("ID Index: " + idIndex);
            System.out.println("ramIndex: " + ramIndex);
            System.out.println("CPU Index: " + cpuIndex);
            System.out.println("Disk Index: " + diskIndex);

            System.out.println("FIELD VALUE IS: " + obj.toString());

          }


      }
      config.put(entry.getKey(), entry.getValue());
    }
    return config;
  }

  private Yaml configYaml() {
    Constructor resourceConfigConstructor = new Constructor(ResourceDefinition.class);
    TypeDescription resourceDescription = new TypeDescription(ResourceDefinition.class);
    resourceConfigConstructor.addTypeDescription(resourceDescription);
    return new Yaml(resourceConfigConstructor);
  }
}


/*
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);


    oos.writeObject(obj);

    oos.flush();
    oos.close();

    InputStream is = new ByteArrayInputStream(baos.toByteArray());



private static Yaml topologyYaml() {
    Constructor topologyConstructor = new Constructor(EcoTopologyDefinition.class);

    TypeDescription topologyDescription = new TypeDescription(EcoTopologyDefinition.class);

    topologyDescription.putListPropertyType("spouts", SpoutDefinition.class);
    topologyDescription.putListPropertyType("bolts", BoltDefinition.class);
    topologyConstructor.addTypeDescription(topologyDescription);

    return new Yaml(topologyConstructor);
  }
 */