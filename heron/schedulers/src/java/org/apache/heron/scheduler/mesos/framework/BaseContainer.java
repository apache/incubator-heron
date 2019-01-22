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

package org.apache.heron.scheduler.mesos.framework;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * BaseContainer describes the basic info for a container
 */

public class BaseContainer {
  public static class EnvironmentVariable {
    @JsonProperty
    public String name;

    @JsonProperty
    public String value;
  }

  @JsonProperty
  public String name;

  @JsonProperty
  public int retries;

  @JsonProperty
  public String runAsUser;

  @JsonProperty
  public String description;

  @JsonProperty
  // # of CPU cores requested
  public double cpu;

  @JsonProperty
  public double diskInMB;

  @JsonProperty
  // in mb
  public double memInMB;

  @JsonProperty
  // # of ports requested
  public int ports;

  @JsonProperty
  public boolean shell;

  @JsonProperty
  public List<String> dependencies = new LinkedList<>();

  @JsonProperty
  public List<EnvironmentVariable> environmentVariables = new LinkedList<>();

  public BaseContainer() {
    // This 0-args constructor is necessary otherwise we could not construct BaseContainer via JSON
  }

  @Override
  public String toString() {
    return BaseContainer.getJobDefinitionInJSON(this);
  }

  public static BaseContainer getJobFromJSONString(String jobDefinitionInJSON) {
    try {
      return new ObjectMapper().readValue(jobDefinitionInJSON, BaseContainer.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse the JSON string", e);
    }
  }

  public static String getJobDefinitionInJSON(BaseContainer job) {
    try {
      return new ObjectMapper().writeValueAsString(job);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to convert to JSON string", e);
    }
  }
}
