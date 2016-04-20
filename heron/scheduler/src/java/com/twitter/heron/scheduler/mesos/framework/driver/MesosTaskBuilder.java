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

package com.twitter.heron.scheduler.mesos.framework.driver;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.Environment;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.Value;

import com.twitter.heron.scheduler.mesos.framework.jobs.BaseJob;

public class MesosTaskBuilder {
  public static final String cpusResourceName = "cpus";
  public static final String memResourceName = "mem";
  public static final String diskResourceName = "disk";
  public static final String portResourceName = "ports";
  public static final String taskNameTemplate = "task:%s";
  private static final Logger LOG = Logger.getLogger(MesosTaskBuilder.class.getName());
  //args|command.
  //  e.g. args: -av (async job), verbose mode
  final String executorArgsPattern = "%s|%s";


  Resource scalarResource(String name, Double value) {
    // Added for convenience.  Uses default catch-all role.
    return Resource.newBuilder()
        .setName(name)
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder().setValue(value))
        .setRole("*")
        .build();
  }

  Resource scalarResource(String name, double value, Offer offer) {
    // For a given named resource and value,
    // find and return the role that matches the name and exceeds the value.
    // Give preference to reserved offers first (those whose roles do not match "*")
    List<Resource> reservedResources = new LinkedList<>();
    for (Resource resource : offer.getResourcesList()) {
      if (resource.hasRole() && resource.getRole() != "*") {
        reservedResources.add(resource);
      }
    }

    String role = "*";
    for (Resource resource : reservedResources) {
      if (resource.getName() == name && resource.getScalar().getValue() >= value) {
        role = resource.getRole();
        break;

      }
    }

    return Resource.newBuilder()
        .setName(name)
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder().setValue(value))
        .setRole(role)
        .build();
  }

  Resource rangeResource(String name, long begin, long end, Offer offer) {
    // For a given named resource and value,
    // find and return the role that matches the name and exceeds the value.
    // Give preference to reserved offers first (those whose roles do not match "*")
    List<Resource> reservedResources = new LinkedList<>();
    for (Resource resource : offer.getResourcesList()) {
      if (resource.hasRole() && resource.getRole() != "*") {
        reservedResources.add(resource);
      }
    }

    String role = "*";
    for (Resource resource : reservedResources) {
      if (resource.getName() == name) {
        Protos.Value.Ranges ranges = resource.getRanges();
        for (Protos.Value.Range range : ranges.getRangeList()) {
          if (range.getBegin() <= begin && range.getEnd() >= end) {
            role = resource.getRole();
            break;
          }
        }
      }
    }

    return Resource.newBuilder()
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

  Environment environment(Map<String, String> var) {
    Environment.Builder builder = Environment.newBuilder();

    for (Map.Entry<String, String> kv : var.entrySet()) {
      String key = kv.getKey();
      String value = kv.getValue();
      Environment.Variable variable =
          Environment.Variable.newBuilder().setName(key).setValue(value).build();
      builder.addVariables(variable);
    }

    return builder.build();
  }

  TaskInfo.Builder getMesosTaskInfoBuilder(String taskIdStr, BaseJob baseJob, Offer offer) {
    TaskID taskId = TaskID.newBuilder().setValue(taskIdStr).build();
    TaskInfo.Builder taskInfo = TaskInfo.newBuilder()
        .setName(String.format(taskNameTemplate, baseJob.name))
        .setTaskId(taskId);
    Environment.Builder environment = Environment.newBuilder();

    // If the job defines custom environment variables, add them to the builder
    // Don't add them if they already exist to prevent overwriting the defaults
    Set<String> builtinEnvNames = new HashSet<>();
    for (Environment.Variable variable : environment.getVariablesList()) {
      builtinEnvNames.add(variable.getName());
    }

    for (BaseJob.EnvironmentVariable ev : baseJob.environmentVariables) {
      environment.addVariables(
          Environment.Variable.newBuilder().setName(ev.name).setValue(ev.value));
    }

    CommandInfo.Builder command = CommandInfo.newBuilder();

    List<CommandInfo.URI> uriProtos = new ArrayList<>();
    for (String uri : baseJob.uris) {
      uriProtos.add(CommandInfo.URI.newBuilder()
          .setValue(uri)
          .build());
    }

    command.setValue(baseJob.command)
        .setShell(baseJob.shell)
        .setEnvironment(environment)
        .addAllArguments(baseJob.arguments)
        .addAllUris(uriProtos);

    if (!baseJob.runAsUser.isEmpty()) {
      command.setUser(baseJob.runAsUser);
    }
    taskInfo.setCommand(command);

    taskInfo
        .addResources(scalarResource(cpusResourceName, baseJob.cpu, offer))
        .addResources(scalarResource(memResourceName, baseJob.mem, offer))
        .addResources(scalarResource(diskResourceName, baseJob.disk, offer));


    return taskInfo;
  }

  String getExecutorName(String x) {
    return String.format("%s", x);
  }
}
