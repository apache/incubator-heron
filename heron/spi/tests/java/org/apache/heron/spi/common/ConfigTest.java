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

package org.apache.heron.spi.common;

import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
@PrepareForTest(ConfigLoader.class)
public class ConfigTest {
  private static final String TEST_DATA_PATH =
      "/__main__/heron/spi/tests/java/org/apache/heron/spi/common/testdata";

  private final String heronHome =
      Paths.get(System.getenv("JAVA_RUNFILES"), TEST_DATA_PATH).toString();
  private final String configPath = Paths.get(heronHome, "local").toString();
  private Config rawConfig;

  @Before
  public void setUp() {
    PowerMockito.spy(ConfigLoader.class);
    rawConfig = ConfigLoader.loadConfig(
        heronHome, configPath, "/release/file", "/override/file");
  }

  @Test
  public void testRaw() {
    assertRaw(rawConfig);
  }

  @Test
  public void testLocalThenCluster() {
    Config localConfig = Config.toLocalMode(rawConfig);
    assertLocal(localConfig);
    assertRaw(rawConfig);

    Config clusterConfig = Config.toClusterMode(localConfig);
    assertCluster(clusterConfig);
    assertLocal(localConfig);
    assertRaw(rawConfig);
  }

  @Test
  public void testClusterThenLocal() {
    Config clusterConfig = Config.toClusterMode(rawConfig);
    assertCluster(clusterConfig);
    assertRaw(rawConfig);

    Config localConfig = Config.toLocalMode(clusterConfig);
    assertLocal(localConfig);
    assertCluster(clusterConfig);
    assertCluster(clusterConfig);
    assertRaw(rawConfig);
  }

  @Test
  public void testReturnSame() {
    Config localConfig = Config.toLocalMode(rawConfig);
    assertTrue(localConfig == Config.toLocalMode(localConfig));

    Config clusterConfig = Config.toClusterMode(rawConfig);
    assertTrue(clusterConfig == Config.toClusterMode(clusterConfig));
  }

  @Test
  public void testObjectReuseLocal() {
    Config localConfig = Config.toLocalMode(rawConfig);
    assertTrue(localConfig == Config.toLocalMode(Config.toClusterMode(localConfig)));
  }

  @Test
  public void testObjectReuseCluster() {
    Config clusterConfig = Config.toClusterMode(rawConfig);
    assertTrue(clusterConfig == Config.toClusterMode(Config.toLocalMode(clusterConfig)));
  }

  private void assertRaw(Config config) {
    assertKeyValue(config, Key.HERON_HOME, heronHome);
    assertKeyValue(config, Key.HERON_CONF, configPath);
    assertKeyValue(config, Key.HERON_LIB,       "${HERON_HOME}/lib");
    assertKeyValue(config, Key.SCHEDULER_YAML,  "${HERON_CONF}/scheduler.yaml");
    assertKeyValue(config, Key.EXECUTOR_BINARY, "${HERON_BIN}/heron-executor");
  }

  private void assertLocal(Config config) {
    assertKeyValue(config, Key.HERON_HOME, heronHome);
    assertKeyValue(config, Key.HERON_CONF, configPath);
    assertKeyValue(config, Key.HERON_LIB,       heronHome + "/lib");
    assertKeyValue(config, Key.SCHEDULER_YAML,  configPath + "/scheduler.yaml");
    assertKeyValue(config, Key.EXECUTOR_BINARY, heronHome + "/bin/heron-executor");
  }

  private void assertCluster(Config config) {
    assertKeyValue(config, Key.HERON_HOME, "./heron-core");
    assertKeyValue(config, Key.HERON_CONF, "./heron-conf");
    assertKeyValue(config, Key.HERON_LIB,       "./heron-core/lib");
    assertKeyValue(config, Key.SCHEDULER_YAML,  "./heron-conf/scheduler.yaml");
    assertKeyValue(config, Key.EXECUTOR_BINARY, "./heron-core/bin/heron-executor");
  }

  private static void assertKeyValue(Config config, Key key, String expected) {
    assertEquals("Unexpected value for key " + key, expected, config.get(key));
  }
}
