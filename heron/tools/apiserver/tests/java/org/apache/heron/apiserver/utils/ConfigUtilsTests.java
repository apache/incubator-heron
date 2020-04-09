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

package org.apache.heron.apiserver.utils;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import org.apache.heron.common.basics.Pair;

import static org.junit.Assert.assertEquals;

public class ConfigUtilsTests {

  @Test
  @SuppressWarnings("unchecked")
  public void testCreateOverrides() throws IOException {
    final Properties overrideProperties = createOverrideProperties(
        Pair.create("heron.statemgr.connection.string", "zookeeper:2181"),
        Pair.create("heron.kubernetes.scheduler.uri", "http://127.0.0.1:8001")
    );

    final String overridesPath = ConfigUtils.createOverrideConfiguration(overrideProperties);
    try (Reader reader = Files.newBufferedReader(Paths.get(overridesPath))) {
      final Map<String, Object> overrides =
          (Map<String, Object>) new Yaml(new SafeConstructor()).load(reader);
      assertEquals(overrides.size(), overrideProperties.size());
      for (String key : overrides.keySet()) {
        assertEquals(overrides.get(key), overrideProperties.getProperty(key));
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testStateManagerFileOverrides() throws IOException {
    final Properties overrideProperties = createOverrideProperties(
        Pair.create("heron.statemgr.connection.string", "zookeeper:2181"),
        Pair.create("heron.kubernetes.scheduler.uri", "http://127.0.0.1:8001")
    );

    final String overridesPath = ConfigUtils.createOverrideConfiguration(overrideProperties);

    // write default state manager config
    final Path stateManagerPath = Files.createTempFile("statemgr-", ".yaml");
    stateManagerPath.toFile().deleteOnExit();
    try (Writer writer = Files.newBufferedWriter(stateManagerPath)) {
      final Map<String, String> config = new HashMap<>();
      config.put("heron.statemgr.connection.string", "<host>:<port>");
      new Yaml(new SafeConstructor()).dump(config, writer);
    }

    // apply the overrides
    ConfigUtils.applyOverridesToStateManagerConfig(Paths.get(overridesPath), stateManagerPath);

    try (Reader reader = Files.newBufferedReader(stateManagerPath)) {
      final Map<String, Object> stateManagerWithOverrides =
          (Map<String, Object>) new Yaml(new SafeConstructor()).load(reader);
      assertEquals(stateManagerWithOverrides.size(), 1);
      assertEquals(stateManagerWithOverrides.get("heron.statemgr.connection.string"),
          "zookeeper:2181");
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNoOverridesAppliedToStateManager() throws IOException {
    final Properties overrideProperties = createOverrideProperties(
        Pair.create("heron.kubernetes.scheduler.uri", "http://127.0.0.1:8001")
    );

    final String overridesPath = ConfigUtils.createOverrideConfiguration(overrideProperties);

    // write default state manager config
    final Path stateManagerPath = Files.createTempFile("statemgr-", ".yaml");
    stateManagerPath.toFile().deleteOnExit();
    try (Writer writer = Files.newBufferedWriter(stateManagerPath)) {
      final Map<String, String> config = new HashMap<>();
      config.put("heron.statemgr.connection.string", "<host>:<port>");
      new Yaml(new SafeConstructor()).dump(config, writer);
    }

    // apply the overrides
    ConfigUtils.applyOverridesToStateManagerConfig(Paths.get(overridesPath), stateManagerPath);

    try (Reader reader = Files.newBufferedReader(stateManagerPath)) {
      final Map<String, Object> stateManagerWithOverrides =
          (Map<String, Object>) new Yaml(new SafeConstructor()).load(reader);
      assertEquals(stateManagerWithOverrides.size(), 1);
      assertEquals(stateManagerWithOverrides.get("heron.statemgr.connection.string"),
          "<host>:<port>");
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testApplyOverrides() throws IOException {
    final Properties overrideProperties = createOverrideProperties(
        Pair.create("heron.statemgr.connection.string", "zookeeper:2181"),
        Pair.create("heron.kubernetes.scheduler.uri", "http://127.0.0.1:8001")
    );

    final String overridesPath = ConfigUtils.createOverrideConfiguration(overrideProperties);

    final Map<String, String> overrides = createOverrides(
        Pair.create("my.override.key", "my.override.value")
    );

    ConfigUtils.applyOverrides(Paths.get(overridesPath), overrides);

    final Map<String, String> combinedOverrides = new HashMap<>();
    combinedOverrides.putAll(overrides);
    for (String key : overrideProperties.stringPropertyNames()) {
      combinedOverrides.put(key, overrideProperties.getProperty(key));
    }

    try (Reader reader = Files.newBufferedReader(Paths.get(overridesPath))) {
      final Map<String, Object> newOverrides =
          (Map<String, Object>) new Yaml(new SafeConstructor()).load(reader);
      assertEquals(newOverrides, combinedOverrides);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testApplyEmptyOverrides() throws IOException {
    final Properties overrideProperties = createOverrideProperties(
        Pair.create("heron.statemgr.connection.string", "zookeeper:2181"),
        Pair.create("heron.kubernetes.scheduler.uri", "http://127.0.0.1:8001")
    );

    final String overridesPath = ConfigUtils.createOverrideConfiguration(overrideProperties);

    ConfigUtils.applyOverrides(Paths.get(overridesPath), new HashMap<>());

    final Map<String, String> overrides = new HashMap<>();
    for (String key : overrideProperties.stringPropertyNames()) {
      overrides.put(key, overrideProperties.getProperty(key));
    }

    try (Reader reader = Files.newBufferedReader(Paths.get(overridesPath))) {
      final Map<String, Object> newOverrides =
          (Map<String, Object>) new Yaml(new SafeConstructor()).load(reader);
      assertEquals(newOverrides, overrides);
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, String> createOverrides(Pair<String, String>... keyValues) {
    final Map<String, String> overrides = new HashMap<>();
    for (Pair<String, String> kv : keyValues) {
      overrides.put(kv.first, kv.second);
    }
    return overrides;
  }

  @SuppressWarnings("unchecked")
  private Properties createOverrideProperties(Pair<String, String>... props) {
    final Properties properties = new Properties();
    for (Pair<String, String> prop : props) {
      properties.setProperty(prop.first, prop.second);
    }

    return properties;
  }
}
