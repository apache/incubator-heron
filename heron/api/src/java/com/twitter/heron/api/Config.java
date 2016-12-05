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

package com.twitter.heron.api;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.DatatypeConverter;

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.common.basics.TypeUtils;

/**
 * Topology configs are specified as a plain old map. This class provides a
 * convenient way to create a topology config map by providing setter methods for
 * all the configs that can be set. It also makes it easier to do things like add
 * serializations.
 * <p>
 * <p>Note that you may put other configurations in any of the configs. Heron
 * will ignore anything it doesn't recognize, but your topologies are free to make
 * use of them by reading them in the prepare method of Bolts or the open method of
 * Spouts.
 */
public class Config extends HashMap<String, Object> {
  /**
   * Topology-specific options for the worker child process. This is used in addition to WORKER_CHILDOPTS.
   */
  public static final String TOPOLOGY_WORKER_CHILDOPTS = "topology.worker.childopts";
  /**
   * Per component jvm options.  The format of this flag is something like
   * spout0:jvmopt_for_spout0,spout1:jvmopt_for_spout1. Mostly should be used
   * in conjunction with setComponentJvmOptions(). This is used in addition
   * to TOPOLOGY_WORKER_CHILDOPTS. While TOPOLOGY_WORKER_CHILDOPTS applies for
   * all components, this is per component
   */
  public static final String TOPOLOGY_COMPONENT_JVMOPTS = "topology.component.jvmopts";
  /**
   * How often a tick tuple from the "__system" component and "__tick" stream should be sent
   * to tasks. Meant to be used as a component-specific configuration.
   */
  public static final String TOPOLOGY_TICK_TUPLE_FREQ_SECS = "topology.tick.tuple.freq.secs";
  /**
   * True if Heron should timeout messages or not. Defaults to true. This is meant to be used
   * in unit tests to prevent tuples from being accidentally timed out during the test.
   */
  public static final String TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS = "topology.enable.message.timeouts";
  /**
   * When set to true, Heron will log every message that's emitted.
   */
  public static final String TOPOLOGY_DEBUG = "topology.debug";
  /**
   * The number of stmgr instances that should spin up to service this
   * topology. All the executors will be evenly shared by these stmgrs.
   */
  public static final String TOPOLOGY_STMGRS = "topology.stmgrs";
  /**
   * The maximum amount of time given to the topology to fully process a message
   * emitted by a spout. If the message is not acked within this time frame, Heron
   * will fail the message on the spout. Some spouts implementations will then replay
   * the message at a later time.
   */
  public static final String TOPOLOGY_MESSAGE_TIMEOUT_SECS = "topology.message.timeout.secs";
  /**
   * The per componentparallelism for a component in this topology.
   * Note:- If you are changing this, please change the utils.h as well
   */
  public static final String TOPOLOGY_COMPONENT_PARALLELISM = "topology.component.parallelism";
  /**
   * The maximum number of tuples that can be pending on a spout task at any given time.
   * This config applies to individual tasks, not to spouts or topologies as a whole.
   * <p>
   * A pending tuple is one that has been emitted from a spout but has not been acked or failed yet.
   * Note that this config parameter has no effect for unreliable spouts that don't tag
   * their tuples with a message id.
   */
  public static final String TOPOLOGY_MAX_SPOUT_PENDING = "topology.max.spout.pending";
  /**
   * A list of task hooks that are automatically added to every spout and bolt in the topology. An example
   * of when you'd do this is to add a hook that integrates with your internal
   * monitoring system. These hooks are instantiated using the zero-arg constructor.
   */
  public static final String TOPOLOGY_AUTO_TASK_HOOKS = "topology.auto.task.hooks";
  /**
   * The serialization class that is used to serialize/deserialize tuples
   */
  public static final String TOPOLOGY_SERIALIZER_CLASSNAME = "topology.serializer.classname";
  /**
   * How many executors to spawn for ackers.
   * <p>
   * <p>If this is set to 0, then Heron will immediately ack tuples as soon
   * as they come off the spout, effectively disabling reliability.</p>
   */
  public static final String TOPOLOGY_ENABLE_ACKING = "topology.acking";
  /**
   * Number of cpu cores per container to be reserved for this topology
   */
  public static final String TOPOLOGY_CONTAINER_CPU_REQUESTED = "topology.container.cpu";
  /**
   * Amount of ram per container to be reserved for this topology.
   * In bytes.
   */
  public static final String TOPOLOGY_CONTAINER_RAM_REQUESTED = "topology.container.ram";
  /**
   * Amount of disk per container to be reserved for this topology.
   * In bytes.
   */
  public static final String TOPOLOGY_CONTAINER_DISK_REQUESTED = "topology.container.disk";
  /**
   * Hint for max number of cpu cores per container to be reserved for this topology
   */
  public static final String TOPOLOGY_CONTAINER_MAX_CPU_HINT = "topology.container.max.cpu.hint";
  /**
   * Hint for max amount of ram per container to be reserved for this topology.
   * In bytes.
   */
  public static final String TOPOLOGY_CONTAINER_MAX_RAM_HINT = "topology.container.max.ram.hint";
  /**
   * Hint for max amount of disk per container to be reserved for this topology.
   * In bytes.
   */
  public static final String TOPOLOGY_CONTAINER_MAX_DISK_HINT = "topology.container.max.disk.hint";
  /**
   * Hint for max amount of disk per container to be reserved for this topology.
   * In bytes.
   */
  public static final String TOPOLOGY_CONTAINER_PADDING_PERCENTAGE
      = "topology.container.padding.percentage";
  /**
   * Per component ram requirement.  The format of this flag is something like
   * spout0:12434,spout1:345353,bolt1:545356.
   */
  public static final String TOPOLOGY_COMPONENT_RAMMAP = "topology.component.rammap";
  /**
   * Name of the topology. This config is automatically set by Heron when the topology is submitted.
   */
  public static final String TOPOLOGY_NAME = "topology.name";
  /**
   * Name of the team which owns this topology.
   */
  public static final String TOPOLOGY_TEAM_NAME = "topology.team.name";
  /**
   * Email of the team which owns this topology.
   */
  public static final String TOPOLOGY_TEAM_EMAIL = "topology.team.email";
  /**
   * Cap ticket (if filed) for the topology. If the topology is in prod this has to be set or it
   * cannot be deployed.
   */
  public static final String TOPOLOGY_CAP_TICKET = "topology.cap.ticket";
  /**
   * Project name of the topology, to help us with tagging which topologies are part of which project. For example, if topology A and
   * Topology B are part of the same project, we will like to aggregate them as part of the same project. This is required by Cap team.
   */
  public static final String TOPOLOGY_PROJECT_NAME = "topology.project.name";
  /**
   * Any user defined classpath that needs to be passed to instances should be set in to config
   * through this key. The value will be of the format "cp1:cp2:cp3..."
   */
  public static final String TOPOLOGY_ADDITIONAL_CLASSPATH = "topology.additional.classpath";

  /**
   * Amount of time to wait after deactivating a topology before updating it
   */
  public static final String TOPOLOGY_UPDATE_DEACTIVATE_WAIT_SECS =
      "topology.update.deactivate.wait.secs";
  /**
   * After updating a topology, amount of time to wait for it to come back up before reactivating it
   */
  public static final String TOPOLOGY_UPDATE_REACTIVATE_WAIT_SECS =
      "topology.update.reactivate.wait.secs";

  private static final long serialVersionUID = 2550967708478837032L;
  // We maintain a list of all user exposed vars
  private static Set<String> apiVars = new HashSet<>();

  static {
    apiVars.add(TOPOLOGY_DEBUG);
    apiVars.add(TOPOLOGY_STMGRS);
    apiVars.add(TOPOLOGY_MESSAGE_TIMEOUT_SECS);
    apiVars.add(TOPOLOGY_COMPONENT_PARALLELISM);
    apiVars.add(TOPOLOGY_MAX_SPOUT_PENDING);
    apiVars.add(TOPOLOGY_WORKER_CHILDOPTS);
    apiVars.add(TOPOLOGY_COMPONENT_JVMOPTS);
    apiVars.add(TOPOLOGY_SERIALIZER_CLASSNAME);
    apiVars.add(TOPOLOGY_TICK_TUPLE_FREQ_SECS);
    apiVars.add(TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS);
    apiVars.add(TOPOLOGY_ENABLE_ACKING);
    apiVars.add(TOPOLOGY_CONTAINER_CPU_REQUESTED);
    apiVars.add(TOPOLOGY_CONTAINER_DISK_REQUESTED);
    apiVars.add(TOPOLOGY_CONTAINER_RAM_REQUESTED);
    apiVars.add(TOPOLOGY_CONTAINER_MAX_CPU_HINT);
    apiVars.add(TOPOLOGY_CONTAINER_MAX_DISK_HINT);
    apiVars.add(TOPOLOGY_CONTAINER_MAX_RAM_HINT);
    apiVars.add(TOPOLOGY_CONTAINER_PADDING_PERCENTAGE);
    apiVars.add(TOPOLOGY_COMPONENT_RAMMAP);
    apiVars.add(TOPOLOGY_NAME);
    apiVars.add(TOPOLOGY_TEAM_NAME);
    apiVars.add(TOPOLOGY_TEAM_EMAIL);
    apiVars.add(TOPOLOGY_CAP_TICKET);
    apiVars.add(TOPOLOGY_PROJECT_NAME);
    apiVars.add(TOPOLOGY_ADDITIONAL_CLASSPATH);
    apiVars.add(TOPOLOGY_UPDATE_DEACTIVATE_WAIT_SECS);
    apiVars.add(TOPOLOGY_UPDATE_REACTIVATE_WAIT_SECS);
  }

  public Config() {
    super();
  }

  public Config(Map<String, Object> map) {
    super(map);
  }

  public static void setDebug(Map<String, Object> conf, boolean isOn) {
    conf.put(Config.TOPOLOGY_DEBUG, String.valueOf(isOn));
  }

  public static void setTeamName(Map<String, Object> conf, String teamName) {
    conf.put(Config.TOPOLOGY_TEAM_NAME, teamName);
  }

  public static void setTeamEmail(Map<String, Object> conf, String teamEmail) {
    conf.put(Config.TOPOLOGY_TEAM_EMAIL, teamEmail);
  }

  public static void setTopologyCapTicket(Map<String, Object> conf, String ticket) {
    conf.put(Config.TOPOLOGY_CAP_TICKET, ticket);
  }

  public static void setTopologyProjectName(Map<String, Object> conf, String project) {
    conf.put(Config.TOPOLOGY_PROJECT_NAME, project);
  }

  public static void setNumStmgrs(Map<String, Object> conf, int stmgrs) {
    conf.put(Config.TOPOLOGY_STMGRS, Integer.toString(stmgrs));
  }

  public static void setSerializationClassName(Map<String, Object> conf, String className) {
    conf.put(Config.TOPOLOGY_SERIALIZER_CLASSNAME, className);
  }

  public static void setEnableAcking(Map<String, Object> conf, boolean acking) {
    conf.put(Config.TOPOLOGY_ENABLE_ACKING, String.valueOf(acking));
  }

  public static void setMessageTimeoutSecs(Map<String, Object> conf, int secs) {
    conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, Integer.toString(secs));
  }

  public static void setComponentParallelism(Map<String, Object> conf, int parallelism) {
    conf.put(Config.TOPOLOGY_COMPONENT_PARALLELISM, Integer.toString(parallelism));
  }

  public static void setMaxSpoutPending(Map<String, Object> conf, int max) {
    conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, Integer.toString(max));
  }

  public static void setTickTupleFrequency(Map<String, Object> conf, int seconds) {
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, Integer.toString(seconds));
  }

  public static void setContainerCpuRequested(Map<String, Object> conf, float ncpus) {
    conf.put(Config.TOPOLOGY_CONTAINER_CPU_REQUESTED, Float.toString(ncpus));
  }

  public static void setContainerDiskRequested(Map<String, Object> conf, ByteAmount nbytes) {
    conf.put(Config.TOPOLOGY_CONTAINER_DISK_REQUESTED, Long.toString(nbytes.asBytes()));
  }

  public static void setContainerRamRequested(Map<String, Object> conf, ByteAmount nbytes) {
    conf.put(Config.TOPOLOGY_CONTAINER_RAM_REQUESTED, Long.toString(nbytes.asBytes()));
  }

  public static void setContainerMaxCpuHint(Map<String, Object> conf, float ncpus) {
    conf.put(Config.TOPOLOGY_CONTAINER_MAX_CPU_HINT, Float.toString(ncpus));
  }

  public static void setContainerMaxDiskHint(Map<String, Object> conf, ByteAmount nbytes) {
    conf.put(Config.TOPOLOGY_CONTAINER_MAX_DISK_HINT, Long.toString(nbytes.asBytes()));
  }

  public static void setContainerMaxRamHint(Map<String, Object> conf, ByteAmount nbytes) {
    conf.put(Config.TOPOLOGY_CONTAINER_MAX_RAM_HINT, Long.toString(nbytes.asBytes()));
  }

  public static void setContainerPaddingPercentage(Map<String, Object> conf, int percentage) {
    conf.put(Config.TOPOLOGY_CONTAINER_PADDING_PERCENTAGE, Integer.toString(percentage));
  }

  public static void setComponentRamMap(Map<String, Object> conf, String ramMap) {
    conf.put(Config.TOPOLOGY_COMPONENT_RAMMAP, ramMap);
  }

  public static void setAutoTaskHooks(Map<String, Object> conf, List<String> hooks) {
    conf.put(Config.TOPOLOGY_AUTO_TASK_HOOKS, hooks);
  }

  public static List<String> getAutoTaskHooks(Map<String, Object> conf) {
    return TypeUtils.getListOfStrings(conf.get(Config.TOPOLOGY_AUTO_TASK_HOOKS));
  }

  public static void setComponentRam(Map<String, Object> conf,
                                     String component, ByteAmount ramInBytes) {
    if (conf.containsKey(Config.TOPOLOGY_COMPONENT_RAMMAP)) {
      String oldEntry = (String) conf.get(Config.TOPOLOGY_COMPONENT_RAMMAP);
      String newEntry = String.format("%s,%s:%d", oldEntry, component, ramInBytes.asBytes());
      conf.put(Config.TOPOLOGY_COMPONENT_RAMMAP, newEntry);
    } else {
      String newEntry = String.format("%s:%d", component, ramInBytes.asBytes());
      conf.put(Config.TOPOLOGY_COMPONENT_RAMMAP, newEntry);
    }
  }

  public static void setComponentJvmOptions(
      Map<String, Object> conf,
      String component,
      String jvmOptions) {
    String optsBase64;
    String componentBase64;

    optsBase64 = DatatypeConverter.printBase64Binary(
        jvmOptions.getBytes(StandardCharsets.UTF_8));
    componentBase64 = DatatypeConverter.printBase64Binary(
        component.getBytes(StandardCharsets.UTF_8));

    String oldEntry = (String) conf.get(Config.TOPOLOGY_COMPONENT_JVMOPTS);
    String newEntry;
    if (oldEntry == null) {
      newEntry = String.format("{\"%s\":\"%s\"}", componentBase64, optsBase64);
    } else {
      // To remove the '{' at the start and '}' at the end
      oldEntry = oldEntry.substring(1, oldEntry.length() - 1);
      newEntry = String.format("{%s,\"%s\":\"%s\"}", oldEntry, componentBase64, optsBase64);
    }
    // Format for TOPOLOGY_COMPONENT_JVMOPTS would be a json map like this:
    //  {
    //     "componentNameAInBase64": "jvmOptionsInBase64",
    //     "componentNameBInBase64": "jvmOptionsInBase64"
    //  }
    conf.put(Config.TOPOLOGY_COMPONENT_JVMOPTS, newEntry);

  }

  public void setDebug(boolean isOn) {
    setDebug(this, isOn);
  }

  public void setTeamName(String teamName) {
    setTeamName(this, teamName);
  }

  public void setTeamEmail(String teamEmail) {
    setTeamEmail(this, teamEmail);
  }

  public void setTopologyCapTicket(String ticket) {
    setTopologyCapTicket(this, ticket);
  }

  public void setTopologyProjectName(String project) {
    setTopologyProjectName(this, project);
  }

  public void setNumStmgrs(int stmgrs) {
    setNumStmgrs(this, stmgrs);
  }

  public void setSerializationClassName(String className) {
    setSerializationClassName(this, className);
  }

  public void setEnableAcking(boolean acking) {
    setEnableAcking(this, acking);
  }

  public void setMessageTimeoutSecs(int secs) {
    setMessageTimeoutSecs(this, secs);
  }

  public void setComponentParallelism(int parallelism) {
    setComponentParallelism(this, parallelism);
  }

  public void setMaxSpoutPending(int max) {
    setMaxSpoutPending(this, max);
  }

  public void setTickTupleFrequency(int seconds) {
    setTickTupleFrequency(this, seconds);
  }

  public void setContainerCpuRequested(float ncpus) {
    setContainerCpuRequested(this, ncpus);
  }

  public void setContainerDiskRequested(ByteAmount nbytes) {
    setContainerDiskRequested(this, nbytes);
  }

  public void setContainerRamRequested(ByteAmount nbytes) {
    setContainerRamRequested(this, nbytes);
  }

  public void setContainerMaxCpuHint(float ncpus) {
    setContainerMaxCpuHint(this, ncpus);
  }

  public void setContainerMaxDiskHint(ByteAmount nbytes) {
    setContainerMaxDiskHint(this, nbytes);
  }

  public void setContainerMaxRamHint(ByteAmount nbytes) {
    setContainerMaxRamHint(this, nbytes);
  }

  public void setContainerPaddingPercentage(int percentage) {
    setContainerPaddingPercentage(this, percentage);
  }

  public void setComponentRamMap(String ramMap) {
    setComponentRamMap(this, ramMap);
  }

  public void setComponentRam(String component, ByteAmount ramInBytes) {
    setComponentRam(this, component, ramInBytes);
  }

  public void setUpdateDeactivateWaitDuration(int seconds) {
    put(Config.TOPOLOGY_UPDATE_DEACTIVATE_WAIT_SECS, Integer.toString(seconds));
  }

  public void setUpdateReactivateWaitDuration(int seconds) {
    put(Config.TOPOLOGY_UPDATE_REACTIVATE_WAIT_SECS, Integer.toString(seconds));
  }

  public List<String> getAutoTaskHooks() {
    return getAutoTaskHooks(this);
  }

  public void setAutoTaskHooks(List<String> hooks) {
    setAutoTaskHooks(this, hooks);
  }

  /*
   * Appends the given classpath to the additional classpath config
   */
  public void addClasspath(Map<String, Object> conf, String classpath) {
    String cpKey = Config.TOPOLOGY_ADDITIONAL_CLASSPATH;

    if (conf.containsKey(cpKey)) {
      String newEntry = String.format("%s:%s", conf.get(cpKey), classpath);
      conf.put(cpKey, newEntry);
    } else {
      conf.put(cpKey, classpath);
    }
  }

  public void setComponentJvmOptions(String component, String jvmOptions) {
    setComponentJvmOptions(this, component, jvmOptions);
  }

  public Set<String> getApiVars() {
    return apiVars;
  }
}
