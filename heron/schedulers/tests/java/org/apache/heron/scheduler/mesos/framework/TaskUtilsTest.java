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

import org.junit.Assert;
import org.junit.Test;


public class TaskUtilsTest {
  @Test
  public void testGetTaskInfo() throws Exception {
    final int CONTAINER_INDEX = 1;
    final int ATTEMPT = 1;

    String taskName = TaskUtils.getTaskNameForContainerIndex(CONTAINER_INDEX);
    Assert.assertTrue(
        taskName.startsWith(String.format("container_%d", CONTAINER_INDEX)));
    String taskId = TaskUtils.getTaskId(taskName, ATTEMPT);
    Assert.assertEquals(
        String.format(TaskUtils.TASK_ID_TEMPLATE, taskName, ATTEMPT), taskId);

    // Get info from taskId
    Assert.assertEquals(taskName, TaskUtils.getTaskNameForTaskId(taskId));
    Assert.assertEquals(CONTAINER_INDEX, TaskUtils.getContainerIndexForTaskId(taskId));
    Assert.assertEquals(ATTEMPT, TaskUtils.getAttemptForTaskId(taskId));

    // Valid taskId or not?
    Assert.assertTrue(TaskUtils.isValidTaskId(taskId));
    Assert.assertFalse(TaskUtils.isValidTaskId("invalidTaskId"));
  }
}
