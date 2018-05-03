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

/**
 * Utils class fot mesos task
 */

public final class TaskUtils {
  private TaskUtils() {
    super();
  }

  //TaskIdFormat: TASK_NAME:ATTEMPT
  public static final String TASK_ID_TEMPLATE = "%s:%d";

  // TaskNameFormat: container_${CONTAINER_INDEX}_${SYSTEM_CURRENT_TIME}
  public static final String TASK_NAME_TEMPLATE = "container_%d_%d";

  public static String getTaskNameForContainerIndex(int containerIndex) {
    return String.format(TASK_NAME_TEMPLATE, containerIndex, System.currentTimeMillis());
  }

  public static String getTaskId(String taskName, int attempt) {
    return String.format(TASK_ID_TEMPLATE, taskName, attempt);
  }

  public static boolean isValidTaskId(String taskId) {
    String[] info = taskId.split(":");
    return info.length == 2;
  }

  public static String getTaskNameForTaskId(String taskId) {
    return taskId.split(":")[0];
  }

  public static int getAttemptForTaskId(String taskId) {
    return Integer.parseInt(taskId.split(":")[1]);
  }

  public static int getContainerIndexForTaskId(String taskId) {
    String taskName = getTaskNameForTaskId(taskId);
    return Integer.parseInt(taskName.split("_")[1]);
  }
}
