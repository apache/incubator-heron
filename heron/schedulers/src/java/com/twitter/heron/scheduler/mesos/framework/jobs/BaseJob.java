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

package com.twitter.heron.scheduler.mesos.framework.jobs;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BaseJob {
  public static class EnvironmentVariable {
    @JsonProperty
    public String name;

    @JsonProperty
    public String value;
  }

  @JsonProperty
  public String name;

  @JsonProperty
  public String command;

  @JsonProperty
  public int retries = 2;

  @JsonProperty
  public String owner;

  @JsonProperty
  public String runAsUser;

  @JsonProperty
  public String description;

  @JsonProperty
  public double cpu;

  @JsonProperty
  public double disk;

  @JsonProperty
  public double mem;

  @JsonProperty
  public int ports;

  @JsonProperty
  public boolean shell;

  @JsonProperty
  public List<String> arguments = new LinkedList<>();

  @JsonProperty
  public List<String> uris = new LinkedList<>();

  @JsonProperty
  public List<EnvironmentVariable> environmentVariables = new LinkedList<>();

  public BaseJob() {
    // This is necessary otherwise we could not construct BaseJob by using JSON
  }

  @Override
  public String toString() {
    return BaseJob.getJobDefinitionInJSON(this);
  }

  public static BaseJob getJobFromJSONString(String jobDefinitionInJSON) {
    try {
      return new ObjectMapper().readValue(jobDefinitionInJSON, BaseJob.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse the JSON string", e);
    }
  }

  public static String getJobDefinitionInJSON(BaseJob job) {
    try {
      return new ObjectMapper().writeValueAsString(job);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to convert to JSON string", e);
    }
  }
}
