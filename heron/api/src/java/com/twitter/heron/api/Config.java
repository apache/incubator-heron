package com.twitter.heron.api;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.DatatypeConverter;

/**
 * Topology configs are specified as a plain old map. This class provides a
 * convenient way to create a topology config map by providing setter methods for
 * all the configs that can be set. It also makes it easier to do things like add
 * serializations.
 * <p/>
 * <p>Note that you may put other configurations in any of the configs. Heron
 * will ignore anything it doesn't recognize, but your topologies are free to make
 * use of them by reading them in the prepare method of Bolts or the open method of
 * Spouts. .</p>
 */
public class Config extends HashMap<String, Object> {
  public Config() {
    super();
  }

  public Config(Map map) {
    super(map);
  }

  /**
   * When set to true, Heron will log every message that's emitted.
   */
  public static String TOPOLOGY_DEBUG = "topology.debug";


  /**
   * The number of stmgr instances that should spin up to service this
   * topology. All the executors will be evenly shared by these stmgrs.
   */
  public static String TOPOLOGY_STMGRS = "topology.stmgrs";

  /**
   * The maximum amount of time given to the topology to fully process a message
   * emitted by a spout. If the message is not acked within this time frame, Heron
   * will fail the message on the spout. Some spouts implementations will then replay
   * the message at a later time.
   */
  public static String TOPOLOGY_MESSAGE_TIMEOUT_SECS = "topology.message.timeout.secs";


  /**
   * The per componentparallelism for a component in this topology.
   * Note:- If you are changing this, please change the utils.h as well
   */
  public static String TOPOLOGY_COMPONENT_PARALLELISM = "topology.component.parallelism";

  /**
   * The maximum number of tuples that can be pending on a spout task at any given time.
   * This config applies to individual tasks, not to spouts or topologies as a whole.
   * <p/>
   * A pending tuple is one that has been emitted from a spout but has not been acked or failed yet.
   * Note that this config parameter has no effect for unreliable spouts that don't tag
   * their tuples with a message id.
   */
  public static String TOPOLOGY_MAX_SPOUT_PENDING = "topology.max.spout.pending";

  /**
   * Topology-specific options for the worker child process. This is used in addition to WORKER_CHILDOPTS.
   */
  public static final String TOPOLOGY_WORKER_CHILDOPTS = "topology.worker.childopts";

  /**
   * A list of task hooks that are automatically added to every spout and bolt in the topology. An example
   * of when you'd do this is to add a hook that integrates with your internal
   * monitoring system. These hooks are instantiated using the zero-arg constructor.
   */
  public static String TOPOLOGY_AUTO_TASK_HOOKS = "topology.auto.task.hooks";

  /**
   * Per component jvm options.  The format of this flag is something like
   * spout0:jvmopt_for_spout0,spout1:jvmopt_for_spout1. Mostly should be used
   * in conjunction with setComponentJvmOptions(). This is used in addition
   * to TOPOLOGY_WORKER_CHILDOPTS. While TOPOLOGY_WORKER_CHILDOPTS applies for
   * all components, this is per component
   */
  public static final String TOPOLOGY_COMPONENT_JVMOPTS = "topology.component.jvmopts";

  /**
   * The serialization class that is used to serialize/deserialize tuples
   */
  public static String TOPOLOGY_SERIALIZER_CLASSNAME = "topology.serializer.classname";

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
   * How many executors to spawn for ackers.
   * <p/>
   * <p>If this is set to 0, then Heron will immediately ack tuples as soon
   * as they come off the spout, effectively disabling reliability.</p>
   */
  public static String TOPOLOGY_ENABLE_ACKING = "topology.acking";

  /**
   * Number of cpu cores per container to be reserved for this topology
   */
  public static String TOPOLOGY_CONTAINER_CPU_REQUESTED = "topology.container.cpu";

  /**
   * Amount of ram per container to be reserved for this topology.
   * In bytes.
   */
  public static String TOPOLOGY_CONTAINER_RAM_REQUESTED = "topology.container.ram";

  /**
   * Amount of disk per container to be reserved for this topology.
   * In bytes.
   */
  public static String TOPOLOGY_CONTAINER_DISK_REQUESTED = "topology.container.disk";

  /**
   * Per component ram requirement.  The format of this flag is something like
   * spout0:12434,spout1:345353,bolt1:545356.
   */
  public static String TOPOLOGY_COMPONENT_RAMMAP = "topology.component.rammap";

  /**
   * Name of the topology. This config is automatically set by Heron when the topology is submitted.
   */
  public static String TOPOLOGY_NAME = "topology.name";


  /**
   * Name of the team which owns this topology.
   */
  public static String TOPOLOGY_TEAM_NAME = "topology.team.name";

  /**
   * Email of the team which owns this topology.
   */
  public static String TOPOLOGY_TEAM_EMAIL = "topology.team.email";

  /**
   * Cap ticket (if filed) for the topology. If the topology is in prod this has to be set or it
   * cannot be deployed.
   */
  public static String TOPOLOGY_CAP_TICKET = "topology.cap.ticket";

  /**
   * Project name of the topology, to help us with tagging which topologies are part of which project. For example, if topology A and
   * Topology B are part of the same project, we will like to aggregate them as part of the same project. This is required by Cap team.
   */
  public static String TOPOLOGY_PROJECT_NAME = "topology.project.name";

  /**
   * Any user defined classpath that needs to be passed to instances should be set in to config
   * through this key. The value will be of the format "cp1:cp2:cp3..."
   */
  public static String TOPOLOGY_ADDITIONAL_CLASSPATH = "topology.additional.classpath";

  public static void setDebug(Map conf, boolean isOn) {
    conf.put(Config.TOPOLOGY_DEBUG, String.valueOf(isOn));
  }

  public void setDebug(boolean isOn) {
    setDebug(this, isOn);
  }

  public static void setTeamName(Map conf, String team_name) {
    conf.put(Config.TOPOLOGY_TEAM_NAME, team_name);
  }

  public void setTeamName(String team_name) {
    setTeamName(this, team_name);
  }

  public static void setTeamEmail(Map conf, String team_email) {
    conf.put(Config.TOPOLOGY_TEAM_EMAIL, team_email);
  }

  public void setTeamEmail(String team_email) {
    setTeamEmail(this, team_email);
  }

  public static void setTopologyCapTicket(Map conf, String ticket) {
    conf.put(Config.TOPOLOGY_CAP_TICKET, ticket);
  }

  public void setTopologyCapTicket(String ticket) {
    setTopologyCapTicket(this, ticket);
  }

  public static void setTopologyProjectName(Map conf, String project) {
    conf.put(Config.TOPOLOGY_PROJECT_NAME, project);
  }

  public void setTopologyProjectName(String project) {
    setTopologyProjectName(this, project);
  }

  public static void setNumStmgrs(Map conf, int stmgrs) {
    conf.put(Config.TOPOLOGY_STMGRS, Integer.toString(stmgrs));
  }

  public void setNumStmgrs(int stmgrs) {
    setNumStmgrs(this, stmgrs);
  }

  public static void setSerializationClassName(Map conf, String className) {
    conf.put(Config.TOPOLOGY_SERIALIZER_CLASSNAME, className);
  }

  public void setSerializationClassName(String className) {
    setSerializationClassName(this, className);
  }

  public static void setEnableAcking(Map conf, boolean acking) {
    conf.put(Config.TOPOLOGY_ENABLE_ACKING, String.valueOf(acking));
  }

  public void setEnableAcking(boolean acking) {
    setEnableAcking(this, acking);
  }

  public static void setMessageTimeoutSecs(Map conf, int secs) {
    conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, Integer.toString(secs));
  }

  public void setMessageTimeoutSecs(int secs) {
    setMessageTimeoutSecs(this, secs);
  }

  public static void setComponentParallelism(Map conf, int parallelism) {
    conf.put(Config.TOPOLOGY_COMPONENT_PARALLELISM, Integer.toString(parallelism));
  }

  public void setComponentParallelism(int parallelism) {
    setComponentParallelism(this, parallelism);
  }

  public static void setMaxSpoutPending(Map conf, int max) {
    conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, Integer.toString(max));
  }

  public void setMaxSpoutPending(int max) {
    setMaxSpoutPending(this, max);
  }

  public static void setTickTupleFrequency(Map conf, int seconds) {
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, Integer.toString(seconds));
  }

  public void setTickTupleFrequency(int seconds) {
    setTickTupleFrequency(this, seconds);
  }

  public void setContainerCpuRequested(float ncpus) {
    setContainerCpuRequested(this, ncpus);
  }

  public static void setContainerCpuRequested(Map conf, float ncpus) {
    conf.put(Config.TOPOLOGY_CONTAINER_CPU_REQUESTED, Float.toString(ncpus));
  }

  public void setContainerDiskRequested(long nbytes) {
    setContainerDiskRequested(this, nbytes);
  }

  public static void setContainerDiskRequested(Map conf, long nbytes) {
    conf.put(Config.TOPOLOGY_CONTAINER_DISK_REQUESTED, Long.toString(nbytes));
  }

  public void setContainerRamRequested(long nbytes) {
    setContainerRamRequested(this, nbytes);
  }

  public static void setContainerRamRequested(Map conf, long nbytes) {
    conf.put(Config.TOPOLOGY_CONTAINER_RAM_REQUESTED, Long.toString(nbytes));
  }

  public void setComponentRamMap(String ramMap) {
    setComponentRamMap(this, ramMap);
  }

  public static void setComponentRamMap(Map conf, String ramMap) {
    conf.put(Config.TOPOLOGY_COMPONENT_RAMMAP, ramMap);
  }

  public void setComponentRam(String component, long ramInBytes) {
    setComponentRam(this, component, ramInBytes);
  }

  public static void setAutoTaskHooks(Map conf, List<String> hooks) {
    conf.put(Config.TOPOLOGY_AUTO_TASK_HOOKS, hooks);
  }

  public void setAutoTaskHooks(List<String> hooks) {
    setAutoTaskHooks(this, hooks);
  }

  public static List<String> getAutoTaskHooks(Map conf) {
    return (List<String>) conf.get(Config.TOPOLOGY_AUTO_TASK_HOOKS);
  }

  public List<String> getAutoTaskHooks() {
    return getAutoTaskHooks(this);
  }

  public static void setComponentRam(Map conf, String component, long ramInBytes) {
    if (conf.containsKey(Config.TOPOLOGY_COMPONENT_RAMMAP)) {
      String old_entry = (String) conf.get(Config.TOPOLOGY_COMPONENT_RAMMAP);
      String new_entry = String.format("%s,%s:%d", old_entry, component, ramInBytes);
      conf.put(Config.TOPOLOGY_COMPONENT_RAMMAP, new_entry);
    } else {
      String new_entry = String.format("%s:%d", component, ramInBytes);
      conf.put(Config.TOPOLOGY_COMPONENT_RAMMAP, new_entry);
    }
  }

  /*
   * Appends the given classpath to the additional classpath config
   */
  public void addClasspath(Map conf, String classpath) {
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

  public static void setComponentJvmOptions(Map conf, String component, String jvmOptions) {
    String optsBase64;
    String componentBase64;
    try {
      optsBase64 = DatatypeConverter.printBase64Binary(jvmOptions.getBytes("UTF-8"));
      componentBase64 = DatatypeConverter.printBase64Binary(component.getBytes("UTF-8"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

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
    //  {"componentNameAInBase64": "jvmOptionsInBase64", "componentNameBInBase64": "jvmOptionsInBase64"}
    conf.put(Config.TOPOLOGY_COMPONENT_JVMOPTS, newEntry);

  }

  // We maintain a list of all user exposed vars
  private static Set<String> apiVars = new HashSet<String>();

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
    apiVars.add(TOPOLOGY_COMPONENT_RAMMAP);
    apiVars.add(TOPOLOGY_NAME);
    apiVars.add(TOPOLOGY_TEAM_NAME);
    apiVars.add(TOPOLOGY_TEAM_EMAIL);
    apiVars.add(TOPOLOGY_CAP_TICKET);
    apiVars.add(TOPOLOGY_PROJECT_NAME);
    apiVars.add(TOPOLOGY_ADDITIONAL_CLASSPATH);
  }

  public Set<String> getApiVars() {
    return apiVars;
  }
}
