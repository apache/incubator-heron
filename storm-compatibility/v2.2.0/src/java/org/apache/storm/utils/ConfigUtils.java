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

package org.apache.storm.utils;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.heron.api.Config;
import org.apache.storm.hooks.ITaskHookDelegate;

public final class ConfigUtils {

  private ConfigUtils() {
  }

  /**
   * Translate storm config to heron config for topology
   * @param stormConfig the storm config
   * @return a heron config
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static Config translateConfig(Map stormConfig) {
    Config heronConfig;
    if (stormConfig != null) {
      heronConfig = new Config((Map<String, Object>) stormConfig);
    } else {
      heronConfig = new Config();
    }

    // Look at serialization stuff first
    doSerializationTranslation(heronConfig);

    // Now look at supported apis
    doStormTranslation(heronConfig);

    doTaskHooksTranslation(heronConfig);

    doTopologyLevelTranslation(heronConfig);

    return heronConfig;
  }

  /**
   * Translate storm config to heron config for components
   * @param stormConfig the storm config
   * @return a heron config
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static Config translateComponentConfig(Map stormConfig) {
    Config heronConfig;
    if (stormConfig != null) {
      heronConfig = new Config((Map<String, Object>) stormConfig);
    } else {
      heronConfig = new Config();
    }

    doStormTranslation(heronConfig);

    return heronConfig;
  }

  private static void doSerializationTranslation(Config heronConfig) {
    // Serialization config is handled by HeronPluggableSerializerDelegate therefore the storm
    // configs are used here instead of translating to the Heron configs. Storm relies on Kryo but
    // Heron abstracts serailizers differently. KryoSerializer is one of the PluggableSerializer.
    if (heronConfig.containsKey(org.apache.storm.Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION)
        && (heronConfig.get(org.apache.storm.Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION)
        instanceof Boolean)
        && ((Boolean)
        heronConfig.get(org.apache.storm.Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION))) {
      org.apache.heron.api.Config.setSerializationClassName(heronConfig,
          "org.apache.heron.api.serializer.JavaSerializer");
    } else {
      heronConfig.put(org.apache.storm.Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, false);
      org.apache.heron.api.Config.setSerializationClassName(heronConfig,
          "org.apache.storm.serialization.HeronPluggableSerializerDelegate");
      if (!heronConfig.containsKey(org.apache.storm.Config.TOPOLOGY_KRYO_FACTORY)) {
        heronConfig.put(org.apache.storm.Config.TOPOLOGY_KRYO_FACTORY,
            "org.apache.storm.serialization.DefaultKryoFactory");
      } else if (!(heronConfig.get(org.apache.storm.Config.TOPOLOGY_KRYO_FACTORY)
          instanceof String)) {
        throw new RuntimeException(
            org.apache.storm.Config.TOPOLOGY_KRYO_FACTORY + " has to be set to a class name");
      }
      if (!heronConfig.containsKey(
          org.apache.storm.Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS)) {
        heronConfig.put(org.apache.storm.Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, false);
      } else if (!(heronConfig.get(org.apache.storm.Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS)
          instanceof Boolean)) {
        throw new RuntimeException(
            org.apache.storm.Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS
                + " has to be boolean");
      }
    }
  }

  /* We need to play a little dance here.
   * task hooks are a list of class names that need to be invoked at various times during a topology run.
   * However because Heron operates in org.apache world and Strom in org.apache.storm world, we
   * pass a ITaskHookDelegate to Heron and remember the actual task hooks in an internal
   * variable STORMCOMPAT_TOPOLOGY_AUTO_TASK_HOOKS
   */
  private static void doTaskHooksTranslation(Config heronConfig) {
    List<String> hooks = heronConfig.getAutoTaskHooks();
    if (hooks != null && !hooks.isEmpty()) {
      heronConfig.put(org.apache.storm.Config.STORMCOMPAT_TOPOLOGY_AUTO_TASK_HOOKS, hooks);
      List<String> translationHooks = new LinkedList<String>();
      translationHooks.add(ITaskHookDelegate.class.getName());
      heronConfig.setAutoTaskHooks(translationHooks);
    }
  }

  /**
   * Translate storm config into heron config. This funciton is used by both topology
   * and component level config translations. Therefore NO config should be generated
   * when a key does NOT exist if the key is for both topology and component.
   * Otherwise the component config might overwrite the topolgy config with a wrong value.
   * @param heron the heron config object to receive the results.
   */
  private static void doStormTranslation(Config heronConfig) {
    if (heronConfig.containsKey(org.apache.storm.Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS)) {
      heronConfig.put(org.apache.storm.Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS,
          heronConfig.get(org.apache.storm.Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS).toString());
    }
    if (heronConfig.containsKey(org.apache.storm.Config.TOPOLOGY_WORKERS)) {
      Integer nWorkers = Utils.getInt(heronConfig.get(org.apache.storm.Config.TOPOLOGY_WORKERS));
      org.apache.heron.api.Config.setNumStmgrs(heronConfig, nWorkers);
    }
    if (heronConfig.containsKey(org.apache.storm.Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)) {
      Integer nSecs =
          Utils.getInt(heronConfig.get(org.apache.storm.Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS));
      org.apache.heron.api.Config.setMessageTimeoutSecs(heronConfig, nSecs);
    }
    if (heronConfig.containsKey(org.apache.storm.Config.TOPOLOGY_MAX_SPOUT_PENDING)) {
      Integer nPending =
          Utils.getInt(
              heronConfig.get(org.apache.storm.Config.TOPOLOGY_MAX_SPOUT_PENDING).toString());
      org.apache.heron.api.Config.setMaxSpoutPending(heronConfig, nPending);
    }
    if (heronConfig.containsKey(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS)) {
      Integer tSecs =
          Utils.getInt(
              heronConfig.get(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS).toString());
      org.apache.heron.api.Config.setTickTupleFrequency(heronConfig, tSecs);
    }
    if (heronConfig.containsKey(org.apache.storm.Config.TOPOLOGY_DEBUG)) {
      Boolean dBg =
          Boolean.parseBoolean(heronConfig.get(org.apache.storm.Config.TOPOLOGY_DEBUG).toString());
      org.apache.heron.api.Config.setDebug(heronConfig, dBg);
    }
    if (heronConfig.containsKey(org.apache.storm.Config.TOPOLOGY_ENVIRONMENT)) {
      org.apache.heron.api.Config.setEnvironment(heronConfig,
          (Map) heronConfig.get(org.apache.storm.Config.TOPOLOGY_ENVIRONMENT));
    }
  }

  /**
   * Translate topology config.
   * @param heron the heron config object to receive the results.
   */
  private static void doTopologyLevelTranslation(Config heronConfig) {
    if (heronConfig.containsKey(org.apache.storm.Config.TOPOLOGY_ACKER_EXECUTORS)) {
      Integer nAckers =
          Utils.getInt(heronConfig.get(org.apache.storm.Config.TOPOLOGY_ACKER_EXECUTORS));
      if (nAckers > 0) {
        org.apache.heron.api.Config.setTopologyReliabilityMode(heronConfig,
                 org.apache.heron.api.Config.TopologyReliabilityMode.ATLEAST_ONCE);
      } else {
        org.apache.heron.api.Config.setTopologyReliabilityMode(heronConfig,
                 org.apache.heron.api.Config.TopologyReliabilityMode.ATMOST_ONCE);
      }
    } else {
      org.apache.heron.api.Config.setTopologyReliabilityMode(heronConfig,
               org.apache.heron.api.Config.TopologyReliabilityMode.ATMOST_ONCE);
    }
  }
}
