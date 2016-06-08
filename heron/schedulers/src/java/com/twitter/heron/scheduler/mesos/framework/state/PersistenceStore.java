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

package com.twitter.heron.scheduler.mesos.framework.state;

import java.util.Map;

import org.apache.mesos.Protos;

import com.twitter.heron.scheduler.mesos.framework.jobs.BaseJob;
import com.twitter.heron.scheduler.mesos.framework.jobs.BaseTask;

public interface PersistenceStore {

  /**
   * Persists a job in the state abstraction.
   *
   * @param baseJob the jobDefinition to persist
   * @return true if the taskId was saved, false if the taskId couldn't be saved.
   */
  boolean persistJob(BaseJob baseJob);

  /**
   * Saves a Running task info in the state abstraction.
   *
   * @param jobName the name of the job to persist.
   * @param task the running task info of the job
   * @return true if the taskId was saved, false if the taskId couldn't be saved.
   */
  boolean persistTask(String jobName, BaseTask task);

  /**
   * Removes a job in the state abstraction.
   *
   * @param jobName the name of the job to remove.
   * @return true if the job was removed, false if the job couldn't be removed.
   */
  boolean removeJob(String jobName);

  /**
   * Removes a task in the state abstraction.
   *
   * @param taskId the taskId to remove.
   * @return true if the job was removed, false if the job couldn't be removed.
   */
  boolean removeTask(String taskId);

  /**
   * Loads all jobs in the state abstraction.
   *
   * @return An iterable of all jobs' definition,
   * and return an empty iterable if there are no jobs
   */
  Iterable<BaseJob> getJobs();

  /**
   * Loads all jobs in the state abstraction.
   *
   * @return A map from JobName to its running info,
   * and return an empty map if there are no running tasks
   */
  Map<String, BaseTask> getTasks();

  /**
   * Load the frameworkId from the underlying store
   *
   * @return return the frameworkId the framework earlier registered and persisted,
   * or null if no frameworkId has ever been persisted.
   */
  Protos.FrameworkID getFrameworkID();

  /**
   * Persists a FrameworkID in the state abstraction.
   *
   * @param frameworkID the FrameworkID to persist
   * @return true if the FrameworkID was saved, false if the FrameworkID couldn't be saved.
   */
  boolean persistFrameworkID(Protos.FrameworkID frameworkID);

  /**
   * Removes the FrameworkId in the state abstraction.
   *
   * @return true if the FrameworkId was removed, false if the FrameworkId couldn't be removed.
   */
  boolean removeFrameworkID();

  /**
   * Clean the state abstraction
   */
  boolean clean();
}
