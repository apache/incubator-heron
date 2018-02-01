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

  public static final String COMPONENT_RESOURCE_MAP = "topology.component.resourcemap";
  public static final String COMPONENT_JVM_OPTIONS = "topology.component.jvmoptions";
  private static final String ID = "id";
  private static final String RAM = "ram";
  private static final String CPU = "cpu";
  private static final String DISK = "disk";
  private static final String EQUALS = "=";
  private static final String WHITESPACE = " ";
  private static final String COMMA = ",";
  private static final String LEFT_BRACKET = "{";
  private static final String RIGHT_BRACKET = "}";
  private static final String MB = "MB";
  private static final String GB = "GB";
  private static final String B = "B";

  protected Config buildConfig(EcoTopologyDefinition topologyDefinition) throws Exception {

    Map<String, Object> configMap = topologyDefinition.getConfig();
    Config config = new Config();
    for (Map.Entry<String, Object> entry: configMap.entrySet()) {

      if (entry.getKey().equals(COMPONENT_RESOURCE_MAP)) {

        setComponentLevelResource(config, entry);

      } else if (entry.getKey().equals(COMPONENT_JVM_OPTIONS)) {

        System.out.println("JVM OPTIONS: " + entry.getValue());

      } else {
        config.put(entry.getKey(), entry.getValue());
      }

    }
    return config;
  }


  private void setComponentLevelResource(Config config, Map.Entry<String, Object> entry) {
    List<Object> objects = (List<Object>) entry.getValue();
    for (Object obj: objects) {

      String objString = obj.toString();

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

      ByteAmount byteAmount = null;

      if (ramWithUom.contains(MB)) {
        // its megaBytes
        int mbIndex = verifyStartingIndexOfUom(ramWithUom, MB);
        byteAmount = ByteAmount.fromMegabytes(extractRawValue(ramWithUom, mbIndex));

      } else if (ramWithUom.contains(GB)) {
        // its gigaBytes
        int gbIndex = verifyStartingIndexOfUom(ramWithUom, GB);
        byteAmount = ByteAmount.fromGigabytes(extractRawValue(ramWithUom, gbIndex));

      } else if (ramWithUom.contains(B)) {
        // its bytes
        int bIndex = verifyStartingIndexOfUom(ramWithUom, B);
        byteAmount = ByteAmount.fromBytes(extractRawValue(ramWithUom, bIndex));

      } else {
        // There is no format throw an exception
        throw new
            IllegalArgumentException(
            " Please specify 'B', 'MB', 'GB' when declaring Ram Resources");
      }

      config.setComponentRam(id, byteAmount);

    }
  }

  private int verifyStartingIndexOfUom(String ramWithUom, String uom) {
    int bIndex = ramWithUom.indexOf(uom);
    String ramUom = ramWithUom.substring(bIndex, ramWithUom.length());
    if (!ramUom.equalsIgnoreCase(uom)) {
      throw new IllegalArgumentException(
          "Unit of Measure must be at the appended at the end of the value.");
    }
    return bIndex;
  }

  private long extractRawValue(String ramWithUom, int index) {

    return Long.valueOf(ramWithUom.substring(0, index));

  }

  private String assignValue(String objString, int index) {
    int equalsIndex = objString.indexOf(EQUALS, index);
    int spaceIndex = objString.indexOf(" ", index);
    return objString.substring(equalsIndex + 1, spaceIndex);
  }
}