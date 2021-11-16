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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.scheduler.utils.SchedulerUtils.ExecutorPort;
import org.apache.heron.spi.common.Config;
import org.apache.mesos.Protos;

/**
 * A structure to group container info and mesos info,
 * representing a task ready to launch
 */

public class LaunchableTask {
  private static final Logger LOG = Logger.getLogger(LaunchableTask.class.getName());

  public final String taskId;
  public final BaseContainer baseContainer;
  public final Protos.Offer offer;
  public final List<Integer> freePorts;

  public LaunchableTask(String taskId, BaseContainer container, Protos.Offer offer,
                        List<Integer> freePorts) {
    this.taskId = taskId;
    this.baseContainer = container;
    this.offer = offer;
    this.freePorts = freePorts;
  }

  protected Protos.Resource scalarResource(String name, double value) {
    // For a given named resource and value,
    // find and return the role that matches the name and exceeds the value.
    // Give preference to reserved offers first (those whose roles do not match "*")
    List<Protos.Resource> reservedResources = new LinkedList<>();
    for (Protos.Resource resource : offer.getResourcesList()) {
      if (resource.hasRole() && !resource.getRole().equals("*")) {
        reservedResources.add(resource);
      }
    }

    String role = "*";
    for (Protos.Resource resource : reservedResources) {
      if (resource.getName().equals(name) && resource.getScalar().getValue() >= value) {
        role = resource.getRole();
        break;

      }
    }

    return Protos.Resource.newBuilder()
        .setName(name)
        .setType(Protos.Value.Type.SCALAR)
        .setScalar(Protos.Value.Scalar.newBuilder().setValue(value))
        .setRole(role)
        .build();
  }

  protected Protos.Resource rangeResource(String name, long begin, long end) {
    // For a given named resource and value,
    // find and return the role that matches the name and exceeds the value.
    // Give preference to reserved offers first (those whose roles do not match "*")
    List<Protos.Resource> reservedResources = new LinkedList<>();
    for (Protos.Resource resource : offer.getResourcesList()) {
      if (resource.hasRole() && !resource.getRole().equals("*")) {
        reservedResources.add(resource);
      }
    }

    String role = "*";
    for (Protos.Resource resource : reservedResources) {
      if (resource.getName().equals(name)) {
        Protos.Value.Ranges ranges = resource.getRanges();
        for (Protos.Value.Range range : ranges.getRangeList()) {
          if (range.getBegin() <= begin && range.getEnd() >= end) {
            role = resource.getRole();
            break;
          }
        }
      }
    }

    return Protos.Resource.newBuilder()
        .setType(Protos.Value.Type.RANGES)
        .setName(name)
        .setRanges(Protos.Value.Ranges.newBuilder()
                .addRange(Protos.Value.Range.newBuilder()
                        .setBegin(begin)
                        .setEnd(end)
                ).build()
        )
        .setRole(role)
        .build();
  }

  protected Protos.Environment environment(Map<String, String> var) {
    Protos.Environment.Builder builder = Protos.Environment.newBuilder();

    for (Map.Entry<String, String> kv : var.entrySet()) {
      String key = kv.getKey();
      String value = kv.getValue();
      Protos.Environment.Variable variable =
          Protos.Environment.Variable.newBuilder().setName(key).setValue(value).build();
      builder.addVariables(variable);
    }

    return builder.build();
  }

  /**
   * Construct the Mesos TaskInfo in Protos to launch basing on the LaunchableTask
   *
   * @param heronConfig the heron config
   * @param heronRuntime the heron runtime
   * @return Mesos TaskInfo in Protos to launch
   */
  public Protos.TaskInfo constructMesosTaskInfo(Config heronConfig, Config heronRuntime) {
    //String taskIdStr, BaseContainer task, Offer offer
    String taskIdStr = this.taskId;

    Protos.TaskID mesosTaskID = Protos.TaskID.newBuilder().setValue(taskIdStr).build();
    Protos.TaskInfo.Builder taskInfo = Protos.TaskInfo.newBuilder()
        .setName(baseContainer.name)
        .setTaskId(mesosTaskID);
    Protos.Environment.Builder environment = Protos.Environment.newBuilder();

    // If the job defines custom environment variables, add them to the builder
    // Don't add them if they already exist to prevent overwriting the defaults
    Set<String> builtinEnvNames = new HashSet<>();
    for (Protos.Environment.Variable variable : environment.getVariablesList()) {
      builtinEnvNames.add(variable.getName());
    }

    for (BaseContainer.EnvironmentVariable ev : baseContainer.environmentVariables) {
      environment.addVariables(
          Protos.Environment.Variable.newBuilder().setName(ev.name).setValue(ev.value));
    }

    taskInfo
        .addResources(scalarResource(TaskResources.CPUS_RESOURCE_NAME, baseContainer.cpu))
        .addResources(scalarResource(TaskResources.MEM_RESOURCE_NAME, baseContainer.memInMB))
        .addResources(scalarResource(TaskResources.DISK_RESOURCE_NAME, baseContainer.diskInMB))
        .addResources(rangeResource(TaskResources.PORT_RESOURCE_NAME,
            this.freePorts.get(0), this.freePorts.get(this.freePorts.size() - 1))).
        setSlaveId(this.offer.getSlaveId());

    int containerIndex = TaskUtils.getContainerIndexForTaskId(taskIdStr);
    String commandStr = executorCommand(heronConfig, heronRuntime, containerIndex);

    Protos.CommandInfo.Builder command = Protos.CommandInfo.newBuilder();

    List<Protos.CommandInfo.URI> uriProtos = new ArrayList<>();
    for (String uri : baseContainer.dependencies) {
      uriProtos.add(Protos.CommandInfo.URI.newBuilder()
          .setValue(uri)
          .setExtract(true)
          .build());
    }

    command.setValue(commandStr)
        .setShell(baseContainer.shell)
        .setEnvironment(environment)
        .addAllUris(uriProtos);

    if (!baseContainer.runAsUser.isEmpty()) {
      command.setUser(baseContainer.runAsUser);
    }
    taskInfo.setCommand(command);

    return taskInfo.build();
  }

  protected String join(String[] array, String delimiter) {
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < array.length - 1; i++) {
      sb.append(array[i]);
      sb.append(delimiter);
    }

    sb.append(array[array.length - 1]);

    return sb.toString();
  }

  protected String executorCommand(
      Config config, Config runtime, int containerIndex) {
    Map<ExecutorPort, String> ports = new HashMap<>();
    ports.put(ExecutorPort.SERVER_PORT, String.valueOf(freePorts.get(0)));
    ports.put(ExecutorPort.TMANAGER_CONTROLLER_PORT, String.valueOf(freePorts.get(1)));
    ports.put(ExecutorPort.TMANAGER_STATS_PORT, String.valueOf(freePorts.get(2)));
    ports.put(ExecutorPort.SHELL_PORT, String.valueOf(freePorts.get(3)));
    ports.put(ExecutorPort.METRICS_MANAGER_PORT, String.valueOf(freePorts.get(4)));
    ports.put(ExecutorPort.SCHEDULER_PORT, String.valueOf(freePorts.get(5)));
    ports.put(ExecutorPort.METRICS_CACHE_SERVER_PORT, String.valueOf(freePorts.get(6)));
    ports.put(ExecutorPort.METRICS_CACHE_STATS_PORT, String.valueOf(freePorts.get(7)));
    ports.put(ExecutorPort.CHECKPOINT_MANAGER_PORT, String.valueOf(freePorts.get(8)));

    String[] executorCmd =
        SchedulerUtils.getExecutorCommand(config, runtime, containerIndex, ports);
    return join(executorCmd, " ");
  }
}
