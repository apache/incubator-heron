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
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNotNull;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
@PrepareForTest(ConfigLoader.class)
public class ConfigLoaderTest {
  private static final String TEST_DATA_PATH =
      "/org_apache_heron/heron/spi/tests/java/org/apache/heron/spi/common/testdata";

  private final String heronHome =
      Paths.get(System.getenv("JAVA_RUNFILES"), TEST_DATA_PATH).toString();
  private final String configPath = Paths.get(heronHome, "local").toString();
  private Config basicConfig;

  @Before
  public void setUp() {
    PowerMockito.spy(ConfigLoader.class);
    basicConfig = Config.toLocalMode(ConfigLoader.loadConfig(
        heronHome, configPath, "/release/file", "/override/file"));
  }

  @Test
  public void testLoadClusterConfig() {
    PowerMockito.spy(ConfigLoader.class);
    Config config = Config.toClusterMode(ConfigLoader.loadConfig(
        heronHome, configPath, "/release/file", "/override/file"));

    assertConfig(config, "./heron-core", "./heron-conf");
  }

  @Test
  public void testLoadDefaultConfig() {
    assertConfig(basicConfig, heronHome, configPath);

    assertKeyValue(basicConfig, Key.PACKING_CLASS,
        "org.apache.heron.packing.roundrobin.RoundRobinPacking");
    assertKeyValue(basicConfig, Key.SCHEDULER_CLASS,
        "org.apache.heron.scheduler.local.LocalScheduler");
    assertKeyValue(basicConfig, Key.LAUNCHER_CLASS,
        "org.apache.heron.scheduler.local.LocalLauncher");
    assertKeyValue(basicConfig, Key.STATE_MANAGER_CLASS,
        "org.apache.heron.state.localfile.LocalFileStateManager");
    assertKeyValue(basicConfig, Key.UPLOADER_CLASS,
        "org.apache.heron.uploader.localfs.FileSystemUploader");
  }

  private static void assertConfig(Config config,
                                   String heronHome,
                                   String heronConfigPath) {
    // assert that the config filenames passed to loadConfig are never null. If they are, the
    // configs defaults are not producing the config files.
    PowerMockito.verifyStatic(times(11));
    ConfigLoader.loadConfig(isNotNull(String.class));
    PowerMockito.verifyStatic(never());
    ConfigLoader.loadConfig(isNull(String.class));

    // addFromFile with an empty map means that the config file was not found. Of the 9 files that
    // are attempted to be loaded, all but 3 should be found (clientConfig, overrideConfigFile and
    // releaseFile do not exist)
    PowerMockito.verifyStatic(times(4));
    ConfigLoader.addFromFile(eq(new HashMap<String, Object>()));

    Set<String> tokenizedValues = new TreeSet<>();
    for (Key key : Key.values()) {
      if (key.getType() == Key.Type.STRING) {
        String value = config.getStringValue(key);
        // assert all tokens got replaced, except JAVA_HOME which might not be set on CI hosts
        if (value != null && value.contains("${") && !value.contains("${JAVA_HOME}")) {
          tokenizedValues.add(value);
        }
      }
    }
    assertTrue("Default config values have not all had tokens replaced: " + tokenizedValues,
        tokenizedValues.isEmpty());
    assertKeyValue(config, Key.HERON_HOME, heronHome);
    assertKeyValue(config, Key.HERON_CONF, heronConfigPath);
    assertKeyValue(config, Key.HERON_BIN, heronHome + "/bin");
    assertKeyValue(config, Key.HERON_DIST, heronHome + "/dist");
    assertKeyValue(config, Key.HERON_LIB, heronHome + "/lib");
    assertKeyValue(config, Key.HERON_ETC, heronHome + "/etc");
    assertKeyValue(config, Key.CLUSTER_YAML, heronConfigPath + "/cluster.yaml");
    assertKeyValue(config, Key.CLIENT_YAML, heronConfigPath + "/client.yaml");
    assertKeyValue(config, Key.METRICS_YAML, heronConfigPath + "/metrics_sinks.yaml");
    assertKeyValue(config, Key.PACKING_YAML, heronConfigPath + "/packing.yaml");
    assertKeyValue(config, Key.SCHEDULER_YAML, heronConfigPath + "/scheduler.yaml");
    assertKeyValue(config, Key.STATEMGR_YAML, heronConfigPath + "/statemgr.yaml");
    assertKeyValue(config, Key.SYSTEM_YAML, heronConfigPath + "/heron_internals.yaml");
    assertKeyValue(config, Key.UPLOADER_YAML, heronConfigPath + "/uploader.yaml");
    assertKeyValue(config, Key.DOWNLOADER_YAML, heronConfigPath + "/downloader.yaml");

    String binPath = config.getStringValue(Key.HERON_BIN);
    assertKeyValue(config, Key.EXECUTOR_BINARY, binPath + "/heron-executor");
    assertKeyValue(config, Key.STMGR_BINARY, binPath + "/heron-stmgr");
    assertKeyValue(config, Key.TMASTER_BINARY, binPath + "/heron-tmaster");
    assertKeyValue(config, Key.SHELL_BINARY, binPath + "/heron-shell");
    assertKeyValue(config, Key.PYTHON_INSTANCE_BINARY, binPath + "/heron-python-instance");
    assertKeyValue(config, Key.CPP_INSTANCE_BINARY, binPath + "/heron-cpp-instance");

    String libPath = config.getStringValue(Key.HERON_LIB);
    assertKeyValue(config, Key.SCHEDULER_JAR, libPath + "/scheduler/heron-scheduler.jar");
    assertKeyValue(config, Key.INSTANCE_CLASSPATH, libPath + "/instance/*");
    assertKeyValue(config, Key.METRICSMGR_CLASSPATH, libPath + "/metricsmgr/*");
    assertKeyValue(config, Key.PACKING_CLASSPATH, libPath + "/packing/*");
    assertKeyValue(config, Key.SCHEDULER_CLASSPATH, libPath + "/scheduler/*");
    assertKeyValue(config, Key.STATEMGR_CLASSPATH, libPath + "/statemgr/*");
    assertKeyValue(config, Key.UPLOADER_CLASSPATH, libPath + "/uploader/*");
  }

  private static void assertKeyValue(Config config, Key key, String expected) {
    assertEquals("Unexpected value for key " + key, expected, config.get(key));
  }

  /**
   * Test reading the cluster.yaml file
   */
  @Test
  public void testClusterFile() throws Exception {

    Config props = ConfigLoader.loadConfig(Context.clusterFile(basicConfig));

    assertEquals(4, props.size());
    assertEquals(
        "org.apache.heron.uploader.localfs.FileSystemUploader",
        Context.uploaderClass(props)
    );
  }

  @Test
  public void testSchedulerFile() throws Exception {
    Config props = ConfigLoader.loadConfig(Context.schedulerFile(basicConfig));

    assertEquals(2, props.size());
    assertEquals(
        "org.apache.heron.scheduler.local.LocalScheduler",
        Context.schedulerClass(props)
    );

    assertEquals(
        "org.apache.heron.scheduler.local.LocalLauncher",
        Context.launcherClass(props)
    );
  }

  @Test
  public void testPackingFile() throws Exception {
    Config props = ConfigLoader.loadConfig(Context.packingFile(basicConfig));

    assertEquals(1, props.size());
    assertEquals(
        "org.apache.heron.packing.roundrobin.RoundRobinPacking",
        props.getStringValue("heron.class.packing.algorithm")
    );
  }

  @Test
  public void testUploaderFile() throws Exception {
    Config props = ConfigLoader.loadConfig(Context.uploaderFile(basicConfig));

    assertEquals(2, props.size());
    assertEquals(
        "/vagrant/heron/jobs",
        props.getStringValue("heron.uploader.file.system.path")
    );
  }
}
