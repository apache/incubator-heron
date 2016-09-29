/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.utils;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.storm.hooks.ITaskHookDelegate;

import com.twitter.heron.api.Config;

public final class ConfigUtils {

  private ConfigUtils() {
  }

  /**
   * Translate storm config to heron config
   * @param stormConfig the storm config
   * @return a heron config
   */
  public static Config translateConfig(Map<String, Object> stormConfig) {
    Config heronConfig = new Config(stormConfig);
    // Look at serialization stuff first
    doSerializationTranslation(heronConfig);

    // Now look at supported apis
    if (heronConfig.containsKey(org.apache.storm.Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS)) {
      heronConfig.put(org.apache.storm.Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS,
          heronConfig.get(org.apache.storm.Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS).toString());
    }
    if (heronConfig.containsKey(org.apache.storm.Config.TOPOLOGY_WORKERS)) {
      Integer nWorkers = (Integer) heronConfig.get(org.apache.storm.Config.TOPOLOGY_WORKERS);
      com.twitter.heron.api.Config.setNumStmgrs(heronConfig, nWorkers);
    }
    if (heronConfig.containsKey(org.apache.storm.Config.TOPOLOGY_ACKER_EXECUTORS)) {
      Integer nAckers =
          (Integer) heronConfig.get(org.apache.storm.Config.TOPOLOGY_ACKER_EXECUTORS);
      com.twitter.heron.api.Config.setEnableAcking(heronConfig, nAckers > 0);
    }
    if (heronConfig.containsKey(org.apache.storm.Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)) {
      Integer nSecs =
          (Integer) heronConfig.get(org.apache.storm.Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS);
      com.twitter.heron.api.Config.setMessageTimeoutSecs(heronConfig, nSecs);
    }
    if (heronConfig.containsKey(org.apache.storm.Config.TOPOLOGY_MAX_SPOUT_PENDING)) {
      Integer nPending =
          Integer.parseInt(
              heronConfig.get(org.apache.storm.Config.TOPOLOGY_MAX_SPOUT_PENDING).toString());
      com.twitter.heron.api.Config.setMaxSpoutPending(heronConfig, nPending);
    }
    if (heronConfig.containsKey(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS)) {
      Integer tSecs =
          Integer.parseInt(
              heronConfig.get(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS).toString());
      com.twitter.heron.api.Config.setTickTupleFrequency(heronConfig, tSecs);
    }
    if (heronConfig.containsKey(org.apache.storm.Config.TOPOLOGY_DEBUG)) {
      Boolean dBg =
          Boolean.parseBoolean(heronConfig.get(org.apache.storm.Config.TOPOLOGY_DEBUG).toString());
      com.twitter.heron.api.Config.setDebug(heronConfig, dBg);
    }

    doTaskHooksTranslation(heronConfig);

    return heronConfig;
  }

  private static void doSerializationTranslation(Config heronConfig) {
    if (heronConfig.containsKey(org.apache.storm.Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION)
        && (heronConfig.get(org.apache.storm.Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION)
        instanceof Boolean)
        && ((Boolean)
        heronConfig.get(org.apache.storm.Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION))) {
      com.twitter.heron.api.Config.setSerializationClassName(heronConfig,
          "com.twitter.heron.api.serializer.JavaSerializer");
    } else {
      heronConfig.put(org.apache.storm.Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, false);
      com.twitter.heron.api.Config.setSerializationClassName(heronConfig,
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
   * However because Heron operates in com.twitter world and Strom in org.apache.storm world, we
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
}
