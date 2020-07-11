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

package backtype.storm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.esotericsoftware.kryo.Serializer;

import backtype.storm.serialization.IKryoDecorator;
import backtype.storm.serialization.IKryoFactory;

/**
 * Topology configs are specified as a plain old map. This class provides a
 * convenient way to create a topology config map by providing setter methods for
 * all the configs that can be set. It also makes it easier to do things like add
 * serializations.
 * <p>
 * This class also provides constants for all the configurations possible on a Storm
 * cluster and Storm topology. Default values for these configs can be found in
 * defaults.yaml.
 * <p>
 * Note that you may put other configurations in any of the configs. Storm
 * will ignore anything it doesn't recognize, but your topologies are free to make
 * use of them by reading them in the prepare method of Bolts or the open method of
 * Spouts.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class Config extends HashMap<String, Object> {
  private static final long serialVersionUID = 2282398261811468412L;

  /**
   * True if Storm should timeout messages or not. Defaults to true. This is meant to be used
   * in unit tests to prevent tuples from being accidentally timed out during the test.
   * Same functionality in Heron.
   */
  public static final String TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS = "topology.enable.message.timeouts";
  /**
   * Whether or not the master should optimize topologies by running multiple
   * tasks in a single thread where appropriate.
   * This has no impact in Heron
   */
  public static final String TOPOLOGY_OPTIMIZE = "topology.optimize";
  /**
   * How many instances to create for a spout/bolt. A task runs on a thread with zero or more
   * other tasks for the same spout/bolt. The number of tasks for a spout/bolt is always
   * the same throughout the lifetime of a topology, but the number of executors (threads) for
   * a spout/bolt can change over time. This allows a topology to scale to more or less resources
   * without redeploying the topology or violating the constraints of Storm (such as a fields grouping
   * guaranteeing that the same value goes to the same task).
   * This has no impact on Heron.
   */
  public static final String TOPOLOGY_TASKS = "topology.tasks";
  /**
   * A list of serialization registrations for Kryo ( http://code.google.com/p/kryo/ ),
   * the underlying serialization framework for Storm. A serialization can either
   * be the name of a class (in which case Kryo will automatically create a serializer for the class
   * that saves all the object's fields), or an implementation of com.esotericsoftware.kryo.Serializer.
   * <p>
   * See Kryo's documentation for more information about writing custom serializers.
   * Same in Heron.
   */
  public static final String TOPOLOGY_KRYO_REGISTER = "topology.kryo.register";
  /**
   * A list of classes that customize storm's kryo instance during start-up.
   * Each listed class name must implement IKryoDecorator. During start-up the
   * listed class is instantiated with 0 arguments, then its 'decorate' method
   * is called with storm's kryo instance as the only argument.
   * Same in Heron.
   */
  public static final String TOPOLOGY_KRYO_DECORATORS = "topology.kryo.decorators";
  /**
   * Class that specifies how to create a Kryo instance for serialization. Storm will then apply
   * topology.kryo.register and topology.kryo.decorators on top of this. The default implementation
   * implements topology.fall.back.on.java.serialization and turns references off.
   * Same in Heron.
   */
  public static final String TOPOLOGY_KRYO_FACTORY = "topology.kryo.factory";
  /**
   * Whether or not Storm should skip the loading of kryo registrations for which it
   * does not know the class or have the serializer implementation. Otherwise, the task will
   * fail to load and will throw an error at runtime. The use case of this is if you want to
   * declare your serializations on the storm.yaml files on the cluster rather than every single
   * time you submit a topology. Different applications may use different serializations and so
   * a single application may not have the code for the other serializers used by other apps.
   * By setting this config to true, Storm will ignore that it doesn't have those other serializations
   * rather than throw an error.
   * Same in Heron.
   */
  public static final String TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS =
      "topology.skip.missing.kryo.registrations";
  /**
   * The maximum amount of time a component gives a source of state to synchronize before it requests
   * synchronization again.
   * This is not implemented in Heron.
   */
  public static final String TOPOLOGY_STATE_SYNCHRONIZATION_TIMEOUT_SECS =
      "topology.state.synchronization.timeout.secs";
  /**
   * Whether or not to use Java serialization in a topology.
   * This has the same meaning in Heron.
   */
  public static final String TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION =
      "topology.fall.back.on.java.serialization";
  /**
   * Topology-specific options for the worker child process. This is used in addition to
   * WORKER_CHILDOPTS.
   * Not yet implemented in Heron.
   */
  public static final String TOPOLOGY_WORKER_CHILDOPTS = "topology.worker.childopts";
  /**
   * This config is available for TransactionalSpouts, and contains the id ( a String) for
   * the transactional topology. This id is used to store the state of the transactional
   * topology in Zookeeper.
   * This is not implemented in Heron.
   */
  public static final String TOPOLOGY_TRANSACTIONAL_ID = "topology.transactional.id";
  /**
   * How often a tick tuple from the "__system" component and "__tick" stream should be sent
   * to tasks. Meant to be used as a component-specific configuration.
   * Same meaning in Heron.
   */
  public static final String TOPOLOGY_TICK_TUPLE_FREQ_SECS = "topology.tick.tuple.freq.secs";
  /**
   * The interval in seconds to use for determining whether to throttle error reported to Zookeeper.
   * For example, an interval of 10 seconds with topology.max.error.report.per.interval set to 5
   * will only allow 5 errors to be reported to Zookeeper per task for every 10 second interval of
   * time.
   * This is not supported in Heron.
   */
  public static final String TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS =
      "topology.error.throttle.interval.secs";
  /**
   * See doc for TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS.
   * This is not supported in Heron
   */
  public static final String TOPOLOGY_MAX_ERROR_REPORT_PER_INTERVAL =
      "topology.max.error.report.per.interval";

  /**
   * When set to true, Storm will log every message that's emitted.
   */
  public static final String TOPOLOGY_DEBUG = "topology.debug";
  /**
   * This currently gets translated to TOPOLOGY_STMGRS. Please see the
   * documentation for TOPOLOGY_STMGRS.
   */
  public static final String TOPOLOGY_WORKERS = "topology.workers";
  /**
   * How many executors to spawn for ackers.
   * <p>If this is set to 0, then Storm will immediately ack tuples as soon
   * as they come off the spout, effectively disabling reliability.
   * In Heron any values of &gt; 0 means to enable acking.
   */
  public static final String TOPOLOGY_ACKER_EXECUTORS = "topology.acker.executors";
  /**
   * The maximum amount of time given to the topology to fully process a message
   * emitted by a spout. If the message is not acked within this time frame, Storm
   * will fail the message on the spout. Some spouts implementations will then replay
   * the message at a later time.
   * This has the same meaning in Heron.
   */
  public static final String TOPOLOGY_MESSAGE_TIMEOUT_SECS = "topology.message.timeout.secs";
  /*
   * A list of classes implementing IMetricsConsumer (See storm.yaml.example for exact config format).
   * Each listed class will be routed all the metrics data generated by the storm metrics API.
   * Each listed class maps 1:1 to a system bolt named __metrics_ClassName#N,
   * and it's parallelism is configurable. This is not supported by Heron.
   */
  public static final String TOPOLOGY_METRICS_CONSUMER_REGISTER =
      "topology.metrics.consumer.register";
  /**
   * The maximum parallelism allowed for a component in this topology. This configuration is
   * typically used in testing to limit the number of threads spawned in simulator.
   * This does not have any impact in Heron
   */
  public static final String TOPOLOGY_MAX_TASK_PARALLELISM = "topology.max.task.parallelism";
  /**
   * The maximum number of tuples that can be pending on a spout task at any given time.
   * This config applies to individual tasks, not to spouts or topologies as a whole.
   * <p>
   * A pending tuple is one that has been emitted from a spout but has not been acked or failed yet.
   * Note that this config parameter has no effect for unreliable spouts that don't tag
   * their tuples with a message id.
   * This has same meaning in Heron.
   */
  public static final String TOPOLOGY_MAX_SPOUT_PENDING = "topology.max.spout.pending";
  /**
   * A class that implements a strategy for what to do when a spout needs to wait. Waiting is
   * triggered in one of two conditions:
   * <p>
   * 1. nextTuple emits no tuples
   * 2. The spout has hit maxSpoutPending and can't emit any more tuples
   * This is not yet implemented in Heron.
   */
  public static final String TOPOLOGY_SPOUT_WAIT_STRATEGY = "topology.spout.wait.strategy";
  /**
   * The amount of milliseconds the SleepEmptyEmitStrategy should sleep for.
   * This is not yet implemented in Heron
   */
  public static final String TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS =
      "topology.sleep.spout.wait.strategy.time.ms";
  /**
   * The percentage of tuples to sample to produce stats for a task.
   * This is not implemented in Heron.
   */
  public static final String TOPOLOGY_STATS_SAMPLE_RATE = "topology.stats.sample.rate";
  /**
   * A list of task hooks that are automatically added to every spout and bolt in the topology.
   * An example of when you'd do this is to add a hook that integrates with your internal
   * monitoring system. These hooks are instantiated using the zero-arg constructor.
   */
  public static final String TOPOLOGY_AUTO_TASK_HOOKS = "topology.auto.task.hooks";
  /**
   * Name of the topology. This config is automatically set by Storm when the topology is submitted.
   * Same in Heron
   */
  public static final String TOPOLOGY_NAME = "topology.name";


  /**
   * Name of the team which owns this topology.
   * Same in Heron
   */
  public static final String TOPOLOGY_TEAM_NAME = "topology.team.name";

  /**
   * Email of the team which owns this topology.
   * Same in Heron
   */
  public static final String TOPOLOGY_TEAM_EMAIL = "topology.team.email";

  /**
   * Cap ticket (if filed) for the topology. If the topology is in prod this has to be set or it
   * cannot be deployed.
   * Same in Heron
   */
  public static final String TOPOLOGY_CAP_TICKET = "topology.cap.ticket";

  /**
   * Project name of the topology, to help us with tagging which topologies are part of which
   * project. For example, if topology A and Topology B are part of the same project, we will
   * like to aggregate them as part of the same project. This is required by Cap team.
   * Same in Heron
   */
  public static final String TOPOLOGY_PROJECT_NAME = "topology.project.name";

  /**
   * A list of hosts of ZooKeeper servers used to manage the cluster.
   */
  public static final String STORM_ZOOKEEPER_SERVERS = "storm.zookeeper.servers";

  /**
   * The port Storm will use to connect to each of the ZooKeeper servers.
   */
  public static final String STORM_ZOOKEEPER_PORT = "storm.zookeeper.port";

  /**
   * The root directory in ZooKeeper for metadata about TransactionalSpouts.
   */
  public static final String TRANSACTIONAL_ZOOKEEPER_ROOT = "transactional.zookeeper.root";

  /**
   * The session timeout for clients to ZooKeeper.
   */
  public static final String STORM_ZOOKEEPER_SESSION_TIMEOUT = "storm.zookeeper.session.timeout";

  /**
   * The connection timeout for clients to ZooKeeper.
   */
  public static final String STORM_ZOOKEEPER_CONNECTION_TIMEOUT =
      "storm.zookeeper.connection.timeout";

  /**
   * The number of times to retry a Zookeeper operation.
   */
  public static final String STORM_ZOOKEEPER_RETRY_TIMES = "storm.zookeeper.retry.times";

  /**
   * The interval between retries of a Zookeeper operation.
   */
  public static final String STORM_ZOOKEEPER_RETRY_INTERVAL = "storm.zookeeper.retry.interval";

  /**
   * The list of zookeeper servers in which to keep the transactional state. If null (which is default),
   * will use storm.zookeeper.servers
   */
  public static final String TRANSACTIONAL_ZOOKEEPER_SERVERS = "transactional.zookeeper.servers";

  /**
   * The port to use to connect to the transactional zookeeper servers. If null (which is default),
   * will use storm.zookeeper.port
   */
  public static final String TRANSACTIONAL_ZOOKEEPER_PORT = "transactional.zookeeper.port";

  /**
   * ----  DO NOT USE -----
   * This variable is used to rewrite the TOPOLOGY_AUTO_TASK_HOOKS variable.
   * As such this is a strictly internal config variable that is not exposed the user
   */
  public static final String STORMCOMPAT_TOPOLOGY_AUTO_TASK_HOOKS =
      "stormcompat.topology.auto.task.hooks";

  public static void setDebug(Map conf, boolean isOn) {
    conf.put(Config.TOPOLOGY_DEBUG, isOn);
  }

  public static void setTeamName(Map conf, String teamName) {
    conf.put(Config.TOPOLOGY_TEAM_NAME, teamName);
  }

  public static void setTeamEmail(Map conf, String teamEmail) {
    conf.put(Config.TOPOLOGY_TEAM_EMAIL, teamEmail);
  }

  public static void setTopologyCapTicket(Map conf, String ticket) {
    conf.put(Config.TOPOLOGY_CAP_TICKET, ticket);
  }

  public static void setTopologyProjectName(Map conf, String project) {
    conf.put(Config.TOPOLOGY_PROJECT_NAME, project);
  }

  public static void setNumWorkers(Map conf, int workers) {
    conf.put(Config.TOPOLOGY_WORKERS, workers);
  }

  public static void setNumAckers(Map conf, int numExecutors) {
    conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, numExecutors);
  }

  public static void setMessageTimeoutSecs(Map conf, int secs) {
    conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, secs);
  }

  public static void registerSerialization(Map conf, Class klass) {
    getRegisteredSerializations(conf).add(klass.getName());
  }

  public static void registerSerialization(
      Map conf, Class klass, Class<? extends Serializer> serializerClass) {
    Map<String, String> register = new HashMap<>();
    register.put(klass.getName(), serializerClass.getName());
    getRegisteredSerializations(conf).add(register);
  }

  public static void registerDecorator(
      Map conf,
      Class<? extends IKryoDecorator> klass) {
    getRegisteredDecorators(conf).add(klass.getName());
  }

  public static void setKryoFactory(Map conf, Class<? extends IKryoFactory> klass) {
    conf.put(Config.TOPOLOGY_KRYO_FACTORY, klass.getName());
  }

  public static void setSkipMissingKryoRegistrations(Map conf, boolean skip) {
    conf.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, skip);
  }

  public static void setMaxTaskParallelism(Map conf, int max) {
    conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, max);
  }

  public static void setMaxSpoutPending(Map conf, int max) {
    conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, max);
  }

  public static void setStatsSampleRate(Map conf, double rate) {
    conf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, rate);
  }

  public static void setFallBackOnJavaSerialization(Map conf, boolean fallback) {
    conf.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, fallback);
  }

  @SuppressWarnings("rawtypes") // List can contain strings or maps
  private static List getRegisteredSerializations(Map conf) {
    List ret;
    if (!conf.containsKey(Config.TOPOLOGY_KRYO_REGISTER)) {
      ret = new ArrayList();
    } else {
      ret = new ArrayList((List) conf.get(Config.TOPOLOGY_KRYO_REGISTER));
    }
    conf.put(Config.TOPOLOGY_KRYO_REGISTER, ret);
    return ret;
  }

  private static List<String> getRegisteredDecorators(Map conf) {
    List<String> ret;
    if (!conf.containsKey(Config.TOPOLOGY_KRYO_DECORATORS)) {
      ret = new ArrayList<>();
    } else {
      ret = new ArrayList<>((List) conf.get(Config.TOPOLOGY_KRYO_DECORATORS));
    }
    conf.put(Config.TOPOLOGY_KRYO_DECORATORS, ret);
    return ret;
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

  /**
   * Set topology optimization
   * @param isOn
   * @deprecated we don't have optimization
   */
  @Deprecated
  public void setOptimize(boolean isOn) {
    put(Config.TOPOLOGY_OPTIMIZE, isOn);
  }

  public void setNumWorkers(int workers) {
    setNumWorkers(this, workers);
  }

  public void setNumAckers(int numExecutors) {
    setNumAckers(this, numExecutors);
  }

  public void setMessageTimeoutSecs(int secs) {
    setMessageTimeoutSecs(this, secs);
  }

  public void registerSerialization(Class klass) {
    registerSerialization(this, klass);
  }

  public void registerSerialization(Class klass, Class<? extends Serializer> serializerClass) {
    registerSerialization(this, klass, serializerClass);
  }

  public void registerMetricsConsumer(Class klass, Object argument, long parallelismHint) {
    HashMap m = new HashMap<>();
    m.put("class", klass.getCanonicalName());
    m.put("parallelism.hint", parallelismHint);
    m.put("argument", argument);

    List l =
        (List) this.get(TOPOLOGY_METRICS_CONSUMER_REGISTER);
    if (l == null) {
      l = new ArrayList<>();
    }
    l.add(m);
    this.put(TOPOLOGY_METRICS_CONSUMER_REGISTER, l);
  }

  public void registerMetricsConsumer(Class klass, long parallelismHint) {
    registerMetricsConsumer(klass, null, parallelismHint);
  }

  public void registerMetricsConsumer(Class klass) {
    registerMetricsConsumer(klass, null, 1L);
  }

  public void registerDecorator(Class<? extends IKryoDecorator> klass) {
    registerDecorator(this, klass);
  }

  public void setKryoFactory(Class<? extends IKryoFactory> klass) {
    setKryoFactory(this, klass);
  }

  public void setSkipMissingKryoRegistrations(boolean skip) {
    setSkipMissingKryoRegistrations(this, skip);
  }

  public void setMaxTaskParallelism(int max) {
    setMaxTaskParallelism(this, max);
  }

  public void setMaxSpoutPending(int max) {
    setMaxSpoutPending(this, max);
  }

  public void setStatsSampleRate(double rate) {
    setStatsSampleRate(this, rate);
  }

  public void setFallBackOnJavaSerialization(boolean fallback) {
    setFallBackOnJavaSerialization(this, fallback);
  }
}

