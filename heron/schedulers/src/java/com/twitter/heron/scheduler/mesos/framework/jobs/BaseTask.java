package com.twitter.heron.scheduler.mesos.framework.jobs;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class BaseTask {
  public enum TaskState {
    TO_SCHEDULE,

    /**
     * ---------------------------------------------------------------
     * States also with related tracking states on mesos side
     * Start
     * ---------------------------------------------------------------
     */
    SCHEDULED,
    TO_KILL,
    TO_KILL_NOW,
    /**
     * ---------------------------------------------------------------
     * States also with related tracking states on mesos side
     * End
     * ---------------------------------------------------------------
     */

    // Terminal states
    FINISHED_SUCCESS,
    FINISHED_FAILURE,
  }

  @JsonProperty
  public String taskId;

  @JsonProperty
  public String slaveId;

  @JsonProperty
  public TaskState state;

  @Override
  public String toString() {
    return BaseTask.getTaskInJSON(this);
  }

  public BaseTask() {
    // This is necessary otherwise we could not construct BaseTask by using fasterxml-jackson
  }

  public BaseTask(String taskId, String slaveId, TaskState state) {
    this.taskId = taskId;
    this.slaveId = slaveId;
    this.state = state;
  }

  public static BaseTask getTaskFromJSONString(String taskInJSON) {
    try {
      return new ObjectMapper().readValue(taskInJSON, BaseTask.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse the JSON string", e);
    }
  }

  public static String getTaskInJSON(BaseTask task) {
    try {
      return new ObjectMapper().writeValueAsString(task);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to convert to JSON string", e);
    }
  }
}
