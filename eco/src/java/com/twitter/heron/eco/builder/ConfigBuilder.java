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

import java.util.List;
import java.util.Map;


import com.twitter.heron.api.Config;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.eco.definition.EcoTopologyDefinition;

@SuppressWarnings("unchecked")
public class ConfigBuilder {

  private static final String COMPONENT_RESOURCE_MAP = "topology.component.resourcemap";
  private static final String ID = "id";
  private static final String RAM = "ram";
  private static final String CPU = "cpu";
  private static final String DISK = "disk";
  private static final String EQUALS = "=";
  private static final String WHITESPACE = " ";
  private static final String COMMA = ",";
  private static final String LEFT_BRACKET = "{";
  private static final String RIGHT_BRACKET = "}";

  protected Config buildConfig(EcoTopologyDefinition topologyDefinition) throws Exception {

    Map<String, Object> configMap = topologyDefinition.getConfig();
    Config config = new Config();
    for (Map.Entry<String, Object> entry: configMap.entrySet()) {

      if (entry.getKey().equals(COMPONENT_RESOURCE_MAP)) {

        List<Object> objects = (List<Object>) entry.getValue();
        for (Object obj: objects) {

          String objString = obj.toString();
          System.out.println("Original objString: " + objString);

          objString = objString.replace(COMMA, WHITESPACE);
          objString = objString.replace(LEFT_BRACKET, WHITESPACE);
          objString = objString.replace(RIGHT_BRACKET, WHITESPACE);

          int idIndex = objString.indexOf(ID);
          int ramIndex = objString.indexOf(RAM);
          int cpuIndex = objString.indexOf(CPU);
          int diskIndex = objString.indexOf(DISK);

          String id = "";
          String ramWithUom = "";
          String diskWithUom = "";
          String cpu = "";

          if (idIndex != -1) {
            id = assignValue(objString, idIndex);
          }

          if (ramIndex != -1) {
            ramWithUom = assignValue(objString, ramIndex);
          }

          if (cpuIndex != -1) {
            cpu = assignValue(objString, cpuIndex);
          }

          if (diskIndex != -1) {
            diskWithUom = assignValue(objString, diskIndex);
          }
          System.out.println("ID " + id);
          System.out.println("RAM " + ramWithUom);
          System.out.println("CPU " + cpu);
          System.out.println("DISK " + diskWithUom);

          if (ramWithUom.contains("MB")) {
            // its megaBytes
            System.out.println("Its megaBytes");
          } else if (ramWithUom.contains("GB")) {
            // its gigaBytes
            System.out.println("Its gigaBytes");
          } else if (ramWithUom.contains("B")) {
            // its bytes
            System.out.println("Its Bytes");
          } else {
            // There is no format throw an exception
          }

          ByteAmount ramInBytes = ByteAmount.fromBytes(11234L);



          if (config.containsKey(Config.TOPOLOGY_COMPONENT_RAMMAP)) {
            String oldEntry = (String) config.get(Config.TOPOLOGY_COMPONENT_RAMMAP);
            String newEntry = String.format("%s,%s:%d", oldEntry, id, ramInBytes.asBytes());
            config.put(Config.TOPOLOGY_COMPONENT_RAMMAP, newEntry);
          } else {
            String newEntry = String.format("%s:%d", id, ramInBytes.asBytes());
            config.put(Config.TOPOLOGY_COMPONENT_RAMMAP, newEntry);
          }


        }

      } else {
        config.put(entry.getKey(), entry.getValue());
      }

    }
    return config;
  }

  private String assignValue(String objString, int idIndex) {
    int equalsIndex = objString.indexOf(EQUALS, idIndex);
    int spaceIndex = objString.indexOf(" ", idIndex);
    return objString.substring(equalsIndex + 1, spaceIndex);
  }
}
