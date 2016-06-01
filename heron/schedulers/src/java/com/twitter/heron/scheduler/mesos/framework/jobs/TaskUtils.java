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

import org.apache.mesos.Protos;

public final class TaskUtils {
  private TaskUtils() {
    super();
  }

  //TaskIdFormat: task:JOB_NAME:DUE:ATTEMPT
  public static final String TASK_ID_TEMPLATE = "task:%s:%d:%d";

  public static Protos.TaskStatus.Builder getMesosTaskStatus(BaseTask task,
                                                             Protos.TaskState state) {
    Protos.TaskStatus.Builder status =
        Protos.TaskStatus.newBuilder().
            setTaskId(Protos.TaskID.newBuilder().setValue(task.taskId)).
            setSlaveId(Protos.SlaveID.newBuilder().setValue(task.slaveId));

    status.setState(state);

    return status;
  }

  public static String getTaskId(BaseJob job, long due, int attempt) {
    return String.format(TASK_ID_TEMPLATE, job.name, due, attempt);
  }

  public static boolean isValidTaskId(String taskId) {
    String[] info = taskId.split(":");
    return info.length == 4;
  }

  public static String getJobNameForTaskId(String taskId) {
    return taskId.split(":")[1];
  }

  public static long getDueTimesForTaskId(String taskId) {
    return Long.parseLong(taskId.split(":")[2]);
  }

  public static int getAttemptForTaskId(String taskId) {
    return Integer.parseInt(taskId.split(":")[3]);
  }
}
