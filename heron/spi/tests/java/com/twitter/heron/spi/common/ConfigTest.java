//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.spi.common;

import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ClusterConfig.class)

public class ConfigTest {
  private static final String TEST_DATA_PATH =
      "/__main__/heron/spi/tests/java/com/twitter/heron/spi/common/testdata";

  private final String heronHome =
      Paths.get(System.getenv("JAVA_RUNFILES"), TEST_DATA_PATH).toString();
  private final String configPath = Paths.get(heronHome, "local").toString();
  private Config rawConfig;

  @Before
  public void setUp() {
    PowerMockito.spy(ClusterConfig.class);
    rawConfig = ClusterConfig.loadConfig(
        heronHome, configPath, "/release/file", "/override/file");
  }

  @Test
  public void testRaw() {
    assertRaw(rawConfig);
  }

  @Test
  public void testLocalThenRemote() {
    Config localConfig = Config.toLocalMode(rawConfig);
    assertLocal(localConfig);
    assertRaw(rawConfig);

    Config remoteConfig = Config.toRemoteMode(localConfig);
    assertRemote(remoteConfig);
    assertLocal(localConfig);
    assertRaw(rawConfig);
  }

  @Test
  public void testRemoteThenLocal() {
    Config remoteConfig = Config.toRemoteMode(rawConfig);
    assertRemote(remoteConfig);
    assertRaw(rawConfig);

    Config localConfig = Config.toLocalMode(remoteConfig);
    assertLocal(localConfig);
    assertRemote(remoteConfig);
    assertRemote(remoteConfig);
    assertRaw(rawConfig);
  }

  @Test
  public void testReturnSame() {
    Config localConfig = Config.toLocalMode(rawConfig);
    assertTrue(localConfig == Config.toLocalMode(localConfig));

    Config remoteConfig = Config.toRemoteMode(rawConfig);
    assertTrue(remoteConfig == Config.toRemoteMode(remoteConfig));
  }

  @Test
  public void testObjectReuseLocal() {
    Config localConfig = Config.toLocalMode(rawConfig);
    assertTrue(localConfig == Config.toLocalMode(Config.toRemoteMode(localConfig)));
  }

  @Test
  public void testObjectReuseRemote() {
    Config remoteConfig = Config.toRemoteMode(rawConfig);
    assertTrue(remoteConfig == Config.toRemoteMode(Config.toLocalMode(remoteConfig)));
  }

  private void assertRaw(Config config) {
    assertKeyValue(config, Key.HERON_HOME, heronHome);
    assertKeyValue(config, Key.HERON_CONF, configPath);
    assertKeyValue(config, Key.HERON_LIB,       "${HERON_HOME}/lib");
    assertKeyValue(config, Key.OVERRIDE_YAML,   "${HERON_CONF}/override.yaml");
    assertKeyValue(config, Key.EXECUTOR_BINARY, "${HERON_BIN}/heron-executor");
  }

  private void assertLocal(Config config) {
    assertKeyValue(config, Key.HERON_HOME, heronHome);
    assertKeyValue(config, Key.HERON_CONF, configPath);
    assertKeyValue(config, Key.HERON_LIB,       heronHome + "/lib");
    assertKeyValue(config, Key.OVERRIDE_YAML,   configPath + "/override.yaml");
    assertKeyValue(config, Key.EXECUTOR_BINARY, heronHome + "/bin/heron-executor");
  }

  private void assertRemote(Config config) {
    assertKeyValue(config, Key.HERON_HOME, "./heron-core");
    assertKeyValue(config, Key.HERON_CONF, "./heron-conf");
    assertKeyValue(config, Key.HERON_LIB,       "./heron-core/lib");
    assertKeyValue(config, Key.OVERRIDE_YAML,   "./heron-conf/override.yaml");
    assertKeyValue(config, Key.EXECUTOR_BINARY, "./heron-core/bin/heron-executor");
  }

  private static void assertKeyValue(Config config, Key key, String expected) {
    assertEquals("Unexpected value for key " + key, expected, config.get(key));
  }
}
