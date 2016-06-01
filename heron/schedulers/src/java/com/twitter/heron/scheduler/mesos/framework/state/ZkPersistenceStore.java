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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.mesos.Protos;

import com.twitter.heron.scheduler.mesos.framework.jobs.BaseJob;
import com.twitter.heron.scheduler.mesos.framework.jobs.BaseTask;
import com.twitter.heron.scheduler.mesos.framework.jobs.TaskUtils;

public class ZkPersistenceStore implements PersistenceStore {
  private static final Logger LOG = Logger.getLogger(ZkPersistenceStore.class.getName());
  private CuratorFramework client;

  private final String connectionString;

  private final String path;
  private final String frameworkIdPath;
  private final String jobPath;
  private final String taskPath;

  public ZkPersistenceStore(String connectionString, int connectionTimeoutMs, int sessionTimeoutMs,
                            String rootPath) {
    // these are reasonable arguments for the ExponentialBackoffRetry. The first
    // retry will wait 1 second - the second will wait up to 2 seconds - the
    // third will wait up to 4 seconds.
    ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);

    this.connectionString = connectionString;

    // using the CuratorFrameworkFactory.builder() gives fine grained control
    // over creation options. See the CuratorFrameworkFactory.Builder javadoc
    // details
    client = CuratorFrameworkFactory.builder()
        .connectString(connectionString)
        .retryPolicy(retryPolicy)
        .connectionTimeoutMs(connectionTimeoutMs)
        .sessionTimeoutMs(sessionTimeoutMs)
            // etc. etc.
        .build();

    // Start it
    client.start();

    path = rootPath;
    frameworkIdPath = path + "/frameworkId";
    jobPath = path + "/jobs";
    taskPath = path + "/tasks";

    // CHECKSTYLE:OFF IllegalCatch
    try {
      if (client.checkExists().forPath(jobPath) == null) {
        client.create().creatingParentsIfNeeded().forPath(jobPath);
      }
      if (client.checkExists().forPath(taskPath) == null) {
        client.create().creatingParentsIfNeeded().forPath(taskPath);
      }
    } catch (Exception e) {
      throw new RuntimeException("Unable to create the path for topology", e);
    }
  }

  @Override
  public boolean persistJob(BaseJob baseJob) {
    LOG.info("Persist Job: " + BaseJob.getJobDefinitionInJSON(baseJob));

    String fullPath = jobPath + "/" + baseJob.name;
    byte[] data = BaseJob.getJobDefinitionInJSON(baseJob).getBytes();
    try {
      if (client.checkExists().forPath(fullPath) != null) {
        LOG.info("Reset the Job data");
        client.setData().forPath(fullPath, data);

        return true;
      }

      // CHECKSTYLE:OFF IllegalCatch
      client.create().creatingParentsIfNeeded().
          forPath(fullPath, data);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to persist job", e);
      return false;
    }

    return true;
  }

  @Override
  public boolean persistTask(String name, BaseTask task) {
    LOG.info(String.format("Persist Task: %s with details: %s", name, task));

    String fullPath = taskPath + "/" + name;
    byte[] data = BaseTask.getTaskInJSON(task).getBytes();
    try {
      if (client.checkExists().forPath(fullPath) != null) {
        LOG.info("Reset the task data");
        client.setData().forPath(fullPath, data);

        return true;
      }

      // CHECKSTYLE:OFF IllegalCatch
      client.create().creatingParentsIfNeeded().
          forPath(fullPath, data);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to persist task", e);
      return false;
    }

    return true;
  }

  @Override
  public boolean removeTask(String taskId) {
    LOG.info("Remove Task: " + taskId);

    // CHECKSTYLE:OFF IllegalCatch
    try {
      client.delete().forPath(taskPath + "/" + TaskUtils.getJobNameForTaskId(taskId));
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to delete task", e);
      return false;
    }

    return true;
  }

  @Override
  public boolean removeJob(String jobName) {
    LOG.info("Remove Job: " + jobName);
    // CHECKSTYLE:OFF IllegalCatch
    try {
      client.delete().forPath(jobPath + "/" + jobName);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to delete job", e);
      return false;
    }

    return true;
  }

  @Override
  public Map<String, BaseTask> getTasks() {
    // TODO(mfu): An inefficient implementation. Optimize if needed.
    LOG.info("Get all Tasks");
    Map<String, BaseTask> tasks = new HashMap<>();

    // CHECKSTYLE:OFF IllegalCatch
    try {
      List<String> tasksName = client.getChildren().forPath(taskPath);
      for (String taskName : tasksName) {
        byte[] res = client.getData().forPath(taskPath + "/" + taskName);

        String taskDefinitionInJSON = new String(res);
        LOG.info("Def: " + taskDefinitionInJSON);
        BaseTask task = BaseTask.getTaskFromJSONString(taskDefinitionInJSON);

        tasks.put(TaskUtils.getJobNameForTaskId(task.taskId), task);
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Unable to get tasks: ", e);
    }

    return tasks;
  }

  @Override
  public Iterable<BaseJob> getJobs() {
    // TODO(mfu): An inefficient implementation. Optimize if needed.
    LOG.info("Get all Jobs");
    List<BaseJob> jobs = new LinkedList<>();

    // CHECKSTYLE:OFF IllegalCatch
    try {
      List<String> jobsName = client.getChildren().forPath(jobPath);
      for (String jobName : jobsName) {
        byte[] res = client.getData().forPath(jobPath + "/" + jobName);

        String jobDefinitionInJSON = new String(res);
        LOG.info("Def: " + jobDefinitionInJSON);
        jobs.add(BaseJob.getJobFromJSONString(jobDefinitionInJSON));
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Unable to get jobs: ", e);
    }

    return jobs;
  }

  @Override
  public Protos.FrameworkID getFrameworkID() {
    LOG.info("Get FrameworkID");

    byte[] res;
    // CHECKSTYLE:OFF IllegalCatch
    try {
      if (client.checkExists().forPath(frameworkIdPath) == null) {
        LOG.info("No existing frameworkId");
        return null;
      }

      res = client.getData().forPath(frameworkIdPath);
    } catch (Exception e) {
      throw new RuntimeException("Failed to read from frameworkId. ", e);
    }

    Protos.FrameworkID frameworkID =
        Protos.FrameworkID.newBuilder().setValue(new String(res)).build();

    return frameworkID;
  }

  @Override
  public boolean persistFrameworkID(Protos.FrameworkID frameworkID) {
    LOG.info("Persist FrameworkId: " + frameworkID);
    // CHECKSTYLE:OFF IllegalCatch
    try {
      if (client.checkExists().forPath(frameworkIdPath) != null) {
        client.setData().forPath(frameworkIdPath, frameworkID.getValue().getBytes());
        return true;
      }

      client.create().creatingParentsIfNeeded().
          forPath(frameworkIdPath, frameworkID.getValue().getBytes());
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to persistent frameworkId", e);
      return false;
    }
    return true;
  }

  @Override
  public boolean removeFrameworkID() {
    LOG.info("Remove frameworkId: " + frameworkIdPath);

    // CHECKSTYLE:OFF IllegalCatch
    try {
      client.delete().forPath(frameworkIdPath);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to remove frameworkId", e);
      return false;
    }

    return true;
  }

  @Override
  public boolean clean() {
    LOG.info("Cleaning all unneeded meta-data");
    LOG.info("Doing some cleaning");

    // CHECKSTYLE:OFF IllegalCatch
    try {
      client.delete().deletingChildrenIfNeeded().forPath(path);
    } catch (Exception e) {
      LOG.info("Unable to clean");
      return false;
    }
    LOG.info("Clean done");

    LOG.info("Closing the CuratorClient");
    client.close();

    return true;
  }

  public String getZookeeperServers() {
    if (connectionString.startsWith("zk://")) {
      return connectionString.replace("zk://", "").replaceAll("/.*", "");
    }
    return connectionString;
  }
}
