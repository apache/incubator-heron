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

package org.apache.heron.eco.definition;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ObjectDefinition {

  private String id;
  private String className;
  private int parallelism = 1;
  private List<Object> constructorArgs;
  private List<PropertyDefinition> properties;
  private List<ConfigurationMethodDefinition> configMethods;
  private boolean hasReferences;

  public List<PropertyDefinition> getProperties() {
    return properties;
  }

  public boolean hasConstructorArgs() {
    return this.constructorArgs != null && this.constructorArgs.size() > 0;
  }

  public void setProperties(List<PropertyDefinition> properties) {
    this.properties = properties;
  }

  public boolean hasReferences() {
    return this.hasReferences;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  public int getParallelism() {
    return parallelism;
  }

  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }

  public List<Object> getConstructorArgs() {
    return constructorArgs;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public void setConstructorArgs(List<Object> constructorArgs) {

    List<Object> newVal = new ArrayList<Object>();
    for (Object obj : constructorArgs) {
      if (obj instanceof LinkedHashMap) {
        Map map = (Map) obj;
        if (map.containsKey("ref") && map.size() == 1) {
          newVal.add(new BeanReference((String) map.get("ref")));
          this.hasReferences = true;
        } else if (map.containsKey("reflist") && map.size() == 1) {
          newVal.add(new BeanListReference((List<String>) map.get("reflist")));
          this.hasReferences = true;
        } else {
          newVal.add(obj);
        }
      } else {
        newVal.add(obj);
      }
    }
    this.constructorArgs = newVal;
  }

  public List<ConfigurationMethodDefinition> getConfigMethods() {
    return configMethods;
  }

  public void setConfigMethods(List<ConfigurationMethodDefinition> configMethods) {
    this.configMethods = configMethods;
  }
}
