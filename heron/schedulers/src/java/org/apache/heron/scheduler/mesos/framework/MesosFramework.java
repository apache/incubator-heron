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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

/**
 * Mesos Framework, communicating with Mesos Master, to do the actual scheduling.
 *
 * We put synchronized block:
 * ```
 * synchronized(this) {...}
 * ```
 * in critical areas for public methods potentially modifying its value.
 * Make sure not acquiring other locks inside this block to prevent the potential for deadlock.
 */

public class MesosFramework implements Scheduler {
  private static final Logger LOG = Logger.getLogger(MesosFramework.class.getName());

  private final Config heronConfig;
  private final Config heronRuntime;

  // ContainerIndex -> BaseContainer
  private final Map<Integer, BaseContainer> containersInfo;

  // ContainerIndex -> TaskId
  private final Map<Integer, String> tasksId;

  // Thread-safe queue of taskId, representing BaseContainer to launch
  private final Queue<String> toScheduleTasks;

  // Flag indicates whether terminated
  private volatile boolean isTerminated;

  // The Mesos' SchedulerDriver
  private volatile SchedulerDriver driver;

  // Indicates whether the MesosFramework has registered.
  // Scheduler.registered(...) method will count down and release the latch
  private final CountDownLatch registeredLatch;

  private volatile Protos.FrameworkID frameworkId;

  public MesosFramework(Config heronConfig, Config heronRuntime) {
    this.heronConfig = heronConfig;
    this.heronRuntime = heronRuntime;
    this.containersInfo = new ConcurrentHashMap<>();
    this.tasksId = new ConcurrentHashMap<>();
    this.toScheduleTasks = new LinkedTransferQueue<>();

    // Initialize it as 1. Will count down and release once got registered
    this.registeredLatch = new CountDownLatch(1);
    this.isTerminated = false;
  }

  // Create a topology
  public boolean createJob(Map<Integer, BaseContainer> jobDefinition) {
    synchronized (this) {
      if (isTerminated) {
        LOG.severe("Job has been killed");
        return false;
      }

      // Record the jobDefinition
      containersInfo.putAll(jobDefinition);

      // To scheduler them with 0 attempt
      for (Map.Entry<Integer, BaseContainer> entry : jobDefinition.entrySet()) {
        Integer containerIndex = entry.getKey();
        BaseContainer container = entry.getValue();
        String taskId = TaskUtils.getTaskId(container.name, 0);
        scheduleNewTask(taskId);
      }
    }

    return true;
  }

  // Kill a topology
  public boolean killJob() {
    synchronized (this) {
      if (isTerminated) {
        LOG.info("Job has been killed");
        return false;
      }
      isTerminated = true;

      LOG.info(String.format("Kill job: %s", Context.topologyName(heronConfig)));
      LOG.info("Remove all tasks to schedule");
      toScheduleTasks.clear();

      // Kill all the tasks
      for (String taskId : tasksId.values()) {
        driver.killTask(Protos.TaskID.newBuilder().setValue(taskId).build());
      }

      containersInfo.clear();
      tasksId.clear();

      return true;
    }
  }

  // Restart a topology
  public boolean restartJob(int containerIndex) {
    synchronized (this) {
      if (isTerminated) {
        LOG.severe("Job has been killed");
        return false;
      }

      // List of tasks to restart
      List<String> tasksToRestart = new ArrayList<>();
      if (containerIndex == -1) {
        // Restart all tasks
        tasksToRestart.addAll(tasksId.values());
      } else {
        tasksToRestart.add(tasksId.get(containerIndex));
      }

      // Kill the task -- the framework will automatically restart it
      for (String taskId : tasksToRestart) {
        driver.killTask(Protos.TaskID.newBuilder().setValue(taskId).build());
      }

      return true;
    }
  }

  /**
   * Check whether the MesosFramework has been terminated
   *
   * @return true if MesosFramework has been terminated
   */
  public boolean isTerminated() {
    return isTerminated;
  }

  /**
   * Causes the current thread to wait for MesosFramework got registered,
   * unless the thread is interrupted, or the specified waiting time elapses.
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @return true MesosFramework got registered,
   * and false if the waiting time elapsed before the count reached zero
   */
  public boolean waitForRegistered(long timeout, TimeUnit unit) {
    try {
      if (this.registeredLatch.await(timeout, unit)) {
        return true;
      }
    } catch (InterruptedException e) {
      LOG.severe("Failed to wait for mesos framework got registered");
      return false;
    }

    return false;
  }

  /**
   * Get the FrameworkID registered with Mesos Master
   *
   * @return the FrameworkID
   */
  public Protos.FrameworkID getFrameworkId() {
    return frameworkId;
  }


  @Override
  public void registered(SchedulerDriver schedulerDriver,
                         Protos.FrameworkID frameworkID,
                         Protos.MasterInfo masterInfo) {
    LOG.info("Registered! ID = " + frameworkID.getValue());
    this.driver = schedulerDriver;
    this.frameworkId = frameworkID;

    // Release the latch
    this.registeredLatch.countDown();
  }

  @Override
  public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {
    LOG.info("Re-registered!");
  }

  @Override
  public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> offers) {
    synchronized (this) {
      LOG.fine("Received Resource Offers: " + offers.toString());

      Map<Protos.Offer, TaskResources> offerResources = new HashMap<>();
      // Convert the offers into TasksResources, an utils class.
      for (Protos.Offer offer : offers) {
        offerResources.put(offer, TaskResources.apply(offer, Context.role(heronConfig)));
      }

      // Generate launchable tasks
      List<LaunchableTask> tasksToLaunch = generateLaunchableTasks(offerResources);

      // Launch tasks
      launchTasks(tasksToLaunch);

      // Decline unused offers
      LOG.fine("Declining unused offers.");
      Set<String> usedOffers = new HashSet<>();
      for (LaunchableTask task : tasksToLaunch) {
        usedOffers.add(task.offer.getId().getValue());
      }
      for (Protos.Offer offer : offers) {
        if (!usedOffers.contains(offer.getId().getValue())) {
          schedulerDriver.declineOffer(offer.getId());
        }
      }
    }
  }

  @Override
  public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {
    LOG.info("Offer Rescinded!");
  }

  @Override
  public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {
    synchronized (this) {
      LOG.info(String.format("Received status update [%s]", taskStatus));
      if (isTerminated) {
        LOG.info("The framework terminated. Ignoring any status update.");

        return;
      }

      String taskId = taskStatus.getTaskId().getValue();

      BaseContainer container = containersInfo.get(TaskUtils.getContainerIndexForTaskId(taskId));

      if (container == null) {
        LOG.warning("Received a status update from unknown container. ");

        // TODO(mfu): Don't know what should be done here. Simply ignore this status update for now.
        return;
      } else {
        handleMesosStatusUpdate(taskId, taskStatus);
      }
    }
  }

  /**
   * Handle status update from mesos master
   * 1. It does nothing except logging under states:
   * - TASK_STAGING,
   * - TASK_STARTING,
   * - TASK_RUNNING,
   * <p>
   * 2. It does severe logging under states:
   * - TASK_FINISHED
   * since heron-executor should be long live in normal production running.
   * This state can occur during DEBUG mode. And we would not consider it as failure.
   * <p>
   * 3. It considers the task a failure and tries to restart it under states:
   * - TASK_FAILED
   * - TASK_KILLED
   * - TASK_LOST
   * - TASK_ERROR
   *
   * @param taskId the taskId of container to handle
   * @param status the TasksStatus to handle
   */
  protected void handleMesosStatusUpdate(String taskId,
                                         Protos.TaskStatus status) {
    // We would determine what to do basing on current taskId and mesos status update
    switch (status.getState()) {
      case TASK_STAGING:
        LOG.info(String.format("Task with id '%s' STAGING", status.getTaskId().getValue()));
        LOG.severe("Framework status updates should not use");

        break;
      case TASK_STARTING:
        LOG.info(String.format("Task with id '%s' STARTING", status.getTaskId().getValue()));

        break;
      case TASK_RUNNING:
        LOG.info(String.format("Task with id '%s' RUNNING", status.getTaskId().getValue()));

        break;
      case TASK_FINISHED:
        LOG.info(String.format("Task with id '%s' FINISHED", status.getTaskId().getValue()));
        LOG.severe("Heron job should not finish!");

        break;
      case TASK_FAILED:
        LOG.info(String.format("Task with id '%s' FAILED", status.getTaskId().getValue()));
        handleMesosFailure(taskId);

        break;
      case TASK_KILLED:
        LOG.info("Task killed which is supported running. "
            + "It could be triggered by manual restart command");
        handleMesosFailure(taskId);

        break;
      case TASK_LOST:
        LOG.info(String.format("Task with id '%s' LOST", status.getTaskId().getValue()));
        handleMesosFailure(taskId);

        break;
      case TASK_ERROR:
        LOG.info(String.format("Task with id '%s' ERROR", status.getTaskId().getValue()));
        handleMesosFailure(taskId);

        break;
      default:
        LOG.severe("Unknown TaskState:" + status.getState() + " for taskId: "
            + status.getTaskId().getValue());

        break;
    }
  }


  /**
   * Restart a failed task unless exceeding the retires limitation
   *
   * @param taskId the taskId of container to handle
   */
  protected void handleMesosFailure(String taskId) {
    int attempt = TaskUtils.getAttemptForTaskId(taskId);
    BaseContainer container = containersInfo.get(TaskUtils.getContainerIndexForTaskId(taskId));

    boolean hasAttemptsLeft = attempt < container.retries;

    if (hasAttemptsLeft) {
      LOG.warning(String.format("Retrying task: %s, attempt: %d", container.name, attempt + 1));

      String newTaskId = TaskUtils.getTaskId(container.name, attempt + 1);
      scheduleNewTask(newTaskId);
    } else {
      LOG.severe("Would not restart the job since it is beyond retries: " + attempt);
    }
  }

  /**
   * Schedule a new task
   *
   * @param taskId the taskId of container to handle
   */
  protected boolean scheduleNewTask(String taskId) {
    // Put it to the pending start queue
    LOG.info(String.format("We are to schedule task: [%s]", taskId));

    // Update the tasksId
    int containerIndex = TaskUtils.getContainerIndexForTaskId(taskId);
    tasksId.put(containerIndex, taskId);

    // Re-schedule it
    toScheduleTasks.add(taskId);
    LOG.info(String.format("Added task: %s into the to-schedule-tasks queue: ", taskId));
    return true;
  }

  @Override
  public void frameworkMessage(SchedulerDriver schedulerDriver,
                               Protos.ExecutorID executorID,
                               Protos.SlaveID slaveID,
                               byte[] data) {
    // TODO(mfu): TO handle this message
    LOG.info("Received framework message.");
  }

  @Override
  public void disconnected(SchedulerDriver schedulerDriver) {
    // TODO(mfu): TO handle this failure
    LOG.info("Disconnected!");
  }

  @Override
  public void slaveLost(SchedulerDriver schedulerDriver, Protos.SlaveID slaveID) {
    // TODO(mfu): TO handle this failure
    LOG.info("Slave lost: " + slaveID.toString());
  }

  @Override
  public void executorLost(SchedulerDriver schedulerDriver,
                           Protos.ExecutorID executorID,
                           Protos.SlaveID slaveID,
                           int status) {
    // TODO(mfu): TO handle this failure
    LOG.info(String.format("Executor lost: %s, slave ID: %s, status: %d",
        executorID.toString(), slaveID.toString(), status));
  }

  @Override
  public void error(SchedulerDriver schedulerDriver, String message) {
    // TODO(mfu): TO handle this failure
    LOG.info("Received error: " + message);
  }


  /**
   * Generate launchable tasks basing on offer resources
   *
   * @param offerResources Mapping from Protos.Offer to utils class TaskResources
   * @return list of launchable task
   */
  protected List<LaunchableTask> generateLaunchableTasks(
      Map<Protos.Offer, TaskResources> offerResources) {
    List<LaunchableTask> tasks = new LinkedList<>();

    if (isTerminated) {
      LOG.info("Job has been killed");
      return tasks;
    }

    while (!toScheduleTasks.isEmpty()) {
      String taskId = toScheduleTasks.poll();
      BaseContainer baseContainer =
          containersInfo.get(TaskUtils.getContainerIndexForTaskId(taskId));

      TaskResources neededResources =
          new TaskResources(
              baseContainer.cpu,
              baseContainer.memInMB,
              baseContainer.diskInMB,
              baseContainer.ports);

      boolean isMatched = false;
      Iterator<Map.Entry<Protos.Offer, TaskResources>> it = offerResources.entrySet()
          .iterator();
      while (it.hasNext()) {
        Map.Entry<Protos.Offer, TaskResources> kv = it.next();
        Protos.Offer offer = kv.getKey();
        TaskResources resources = kv.getValue();

        if (resources.canSatisfy(neededResources)) {
          resources.consume(neededResources);

          // Construct the list of free ports to use for a heron executor.
          List<Integer> freePorts = new ArrayList<>();
          for (int port = (int) (neededResources.getPortsHold().get(0).rangeStart);
               port <= (int) (neededResources.getPortsHold().get(0).rangeEnd); port++) {
            freePorts.add(port);
          }

          // Add the tasks
          tasks.add(new LaunchableTask(taskId, baseContainer, offer, freePorts));

          // Set the flag
          isMatched = true;

          // Matched baseContainer, break;
          break;
        }
      }

      if (!isMatched) {
        LOG.info(String.format("Insufficient resources remaining for baseContainer: %s, "
                + "will append to queue. Need: [%s]",
            taskId, neededResources.toString()));
        toScheduleTasks.add(taskId);

        // Offer will not be able to satisfy resources needed for any rest container,
        // since all containers are homogeneous.
        // Break the loop
        break;
      }
    }

    return tasks;
  }

  /**
   * Launch tasks
   *
   * @param tasks list of LaunchableTask
   */
  protected void launchTasks(List<LaunchableTask> tasks) {
    // Group the tasks by offer
    Map<Protos.Offer, List<LaunchableTask>> tasksGroupByOffer = new HashMap<>();
    for (LaunchableTask task : tasks) {
      List<LaunchableTask> subTasks = tasksGroupByOffer.get(task.offer);
      if (subTasks == null) {
        subTasks = new LinkedList<>();
      }
      subTasks.add(task);
      tasksGroupByOffer.put(task.offer, subTasks);
    }

    // Construct the Mesos TaskInfo to launch
    for (Map.Entry<Protos.Offer, List<LaunchableTask>> kv : tasksGroupByOffer.entrySet()) {
      // We would launch taks for the same offer at one shot
      Protos.Offer offer = kv.getKey();
      List<LaunchableTask> subTasks = kv.getValue();

      List<Protos.TaskInfo> mesosTasks = new LinkedList<>();
      for (LaunchableTask task : subTasks) {
        Protos.TaskInfo pTask = task.constructMesosTaskInfo(heronConfig, heronRuntime);
        mesosTasks.add(pTask);
      }

      LOG.info("Launching tasks from offer: " + offer + " with tasks: " + mesosTasks);

      // Launch the tasks for the same offer
      Protos.Status status =
          driver.launchTasks(Arrays.asList(new Protos.OfferID[]{offer.getId()}),
              mesosTasks);

      if (status == Protos.Status.DRIVER_RUNNING) {
        LOG.info(String.format("Tasks launched, status: '%s'", status));

      } else {
        LOG.severe("Other status returned: " + status);

        // Handled failed tasks
        for (LaunchableTask task : tasks) {
          handleMesosFailure(task.taskId);
          // No need to decline the offers; mesos master already reclaim them
        }
      }
    }
  }

  /**
   * Followings are used for unit test only
   */
  protected Map<Integer, BaseContainer> getContainersInfo() {
    return containersInfo;
  }

  protected Map<Integer, String> getTasksId() {
    return tasksId;
  }

  protected Queue<String> getToScheduleTasks() {
    return toScheduleTasks;
  }

  protected SchedulerDriver getSchedulerDriver() {
    return driver;
  }
}
