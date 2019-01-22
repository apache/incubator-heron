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

package org.apache.heron.eco.builder;

import java.util.List;
import java.util.Map;

import org.apache.heron.api.Config;
import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.eco.definition.EcoTopologyDefinition;

@SuppressWarnings("unchecked")
public class ConfigBuilder {

  public static final String COMPONENT_RESOURCE_MAP = "topology.component.resourcemap";
  public static final String COMPONENT_JVM_OPTIONS = "topology.component.jvmoptions";
  private static final String ID = "id";
  private static final String RAM = "ram";
  private static final String CPU = "cpu";
  private static final String DISK = "disk";
  private static final String OPTIONS = "options";
  private static final String EQUALS = "=";
  private static final String WHITESPACE = " ";
  private static final String COMMA = ",";
  private static final String LEFT_BRACE = "{";
  private static final String RIGHT_BRACE = "}";
  private static final String LEFT_BRACKET = "[";
  private static final String RIGHT_BRACKET = "]";
  private static final String MB = "MB";
  private static final String GB = "GB";
  private static final String B = "B";
  private static final Integer MINIMUM_BYTES = 256000000;
  private static final Integer MINIMUM_MB = 256;

  /**
   * Build the config for a ECO topology definition
   *
   * @param topologyDefinition - ECO topology definition
   */
  public Config buildConfig(EcoTopologyDefinition topologyDefinition)
      throws IllegalArgumentException {

    Map<String, Object> configMap = topologyDefinition.getConfig();
    Config config = new Config();
    for (Map.Entry<String, Object> entry: configMap.entrySet()) {

      if (entry.getKey().equals(COMPONENT_RESOURCE_MAP)) {

        setComponentLevelResource(config, entry);

      } else if (entry.getKey().equals(COMPONENT_JVM_OPTIONS)) {

        List<Object> objects = (List<Object>) entry.getValue();
        for (Object obj : objects) {
          String objString = obj.toString();
          objString = objString.replace(LEFT_BRACE, WHITESPACE);
          objString = objString.replace(RIGHT_BRACE, WHITESPACE);

          int idIndex = objString.indexOf(ID);
          int optionsIndex = objString.indexOf(OPTIONS);

          String id = getIdValue(objString, idIndex);

          String jvmOptions;
          if (optionsIndex != -1) {

            int equalsIndex = objString.indexOf(EQUALS, optionsIndex);
            jvmOptions = objString.substring(equalsIndex + 1, objString.length());

            jvmOptions = jvmOptions.replace(LEFT_BRACKET, "")
                .replace(RIGHT_BRACKET, "");

          } else {
            throw new IllegalArgumentException(
                "You must specify the JVM options for your component");
          }

          config.setComponentJvmOptions(id, jvmOptions);
        }

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
      objString = objString.replace(LEFT_BRACE, WHITESPACE);
      objString = objString.replace(RIGHT_BRACE, WHITESPACE);

      int idIndex = objString.indexOf(ID);
      int ramIndex = objString.indexOf(RAM);
      int cpuIndex = objString.indexOf(CPU);
      int diskIndex = objString.indexOf(DISK);

      String ramWithUom = "";
      String id = getIdValue(objString, idIndex);
      //todo (josh fischer) diskWithUom and cpu are still to be implemented for use with k8s
      String diskWithUom = "";
      String cpu = "";

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
        long megaBytes = extractRawValue(ramWithUom, mbIndex);
        if (megaBytes < MINIMUM_MB) {
          throw new IllegalArgumentException(
              "The minimum RAM resource allocation for a component must be at least 256MB");
        }
        byteAmount = ByteAmount.fromMegabytes(megaBytes);

      } else if (ramWithUom.contains(GB)) {
        // its gigaBytes
        // we don't validate here as NumberFormatException is thrown converting decimals to longs
        int gbIndex = verifyStartingIndexOfUom(ramWithUom, GB);
        byteAmount = ByteAmount.fromGigabytes(extractRawValue(ramWithUom, gbIndex));

      } else if (ramWithUom.contains(B)) {
        // its bytes
        int bIndex = verifyStartingIndexOfUom(ramWithUom, B);
        long bytes = extractRawValue(ramWithUom, bIndex);
        if (bytes < MINIMUM_BYTES) {
          throw new IllegalArgumentException(
              "The minimum RAM resource allocation for a component must be at least 256000000B");
        }
        byteAmount = ByteAmount.fromBytes(bytes);

      } else {
        // There is no format throw an exception
        throw new
            IllegalArgumentException(
            " Please specify 'B', 'MB', 'GB' when declaring RAM Resources");
      }
      config.setComponentRam(id, byteAmount);
    }
  }

  private String getIdValue(String objString, int idIndex) {
    String id = "";
    if (idIndex != -1) {
      id = assignValue(objString, idIndex);
    } else {
      throw new IllegalArgumentException("Must specify ID of component to allocate resources");
    }
    return id;
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
