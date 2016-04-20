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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BaseTask {
  @JsonProperty
  public String taskId;
  @JsonProperty
  public String slaveId;
  @JsonProperty
  public TaskState state;

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

  @Override
  public String toString() {
    return BaseTask.getTaskInJSON(this);
  }

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
}
