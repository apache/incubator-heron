package com.twitter.heron.scheduler.mesos.framework.jobs;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

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

  public BaseJob(){
    // This is necessary otherwise we could not construct BaseJob by using JSON
  }

  @Override
  public String toString() {
    return BaseJob.getJobDefinitionInJSON(this);
  }

  public static BaseJob getJobFromJSONString(String JobDefinitionInJSON) {
    try {
      return new ObjectMapper().readValue(JobDefinitionInJSON, BaseJob.class);
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
