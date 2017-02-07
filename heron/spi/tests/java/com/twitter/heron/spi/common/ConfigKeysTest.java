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

package com.twitter.heron.spi.common;

import org.junit.Assert;
import org.junit.Test;

public class ConfigKeysTest {

  @Test
  public void testHeronEnviron() throws Exception {
    assertEquals(
        "heron.directory.home",
        Key.HERON_HOME
    );
    assertEquals(
        "heron.directory.bin",
        Key.HERON_BIN
    );
    assertEquals(
        "heron.directory.conf",
        Key.HERON_CONF
    );
    assertEquals(
        "heron.directory.lib",
        Key.HERON_LIB
    );
    assertEquals(
        "heron.directory.dist",
        Key.HERON_DIST
    );
    assertEquals(
        "heron.directory.etc",
        Key.HERON_ETC
    );
    assertEquals(
        "heron.directory.java.home",
        Key.JAVA_HOME
    );
  }

  @Test
  public void testConfigFiles() throws Exception {
    assertEquals(
        "heron.config.file.cluster.yaml",
        Key.CLUSTER_YAML
    );
    assertEquals(
        "heron.config.file.metrics.yaml",
        Key.METRICS_YAML
    );
    assertEquals(
        "heron.config.file.packing.yaml",
        Key.PACKING_YAML
    );
    assertEquals(
        "heron.config.file.scheduler.yaml",
        Key.SCHEDULER_YAML
    );
    assertEquals(
        "heron.config.file.statemgr.yaml",
        Key.STATEMGR_YAML
    );
    assertEquals(
        "heron.config.file.system.yaml",
        Key.SYSTEM_YAML
    );
    assertEquals(
        "heron.config.file.uploader.yaml",
        Key.UPLOADER_YAML
    );
  }

  @Test
  public void testConfig() throws Exception {
    assertEquals(
        "heron.config.cluster",
        Key.CLUSTER
    );
    assertEquals(
        "heron.config.role",
        Key.ROLE
    );
    assertEquals(
        "heron.config.environ",
        Key.ENVIRON
    );
    assertEquals(
        "heron.config.verbose",
        Key.VERBOSE
    );
    assertEquals(
        "heron.config.path",
        Key.CONFIG_PATH
    );
    assertEquals(
        "heron.config.property",
        Key.CONFIG_PROPERTY
    );
  }

  @Test
  public void testConfigClasses() throws Exception {
    assertEquals(
        "heron.class.uploader",
        Key.UPLOADER_CLASS
    );
    assertEquals(
        "heron.class.launcher",
        Key.LAUNCHER_CLASS
    );
    assertEquals(
        "heron.class.scheduler",
        Key.SCHEDULER_CLASS
    );
    assertEquals(
        "heron.class.packing.algorithm",
        Key.PACKING_CLASS
    );
    assertEquals(
        "heron.class.state.manager",
        Key.STATE_MANAGER_CLASS
    );
  }

  @Test
  public void testBinaries() throws Exception {
    assertEquals(
        "heron.binaries.sandbox.executor",
        Key.SANDBOX_EXECUTOR_BINARY
    );
    assertEquals(
        "heron.binaries.sandbox.stmgr",
        Key.SANDBOX_STMGR_BINARY
    );
    assertEquals(
        "heron.binaries.sandbox.tmaster",
        Key.SANDBOX_TMASTER_BINARY
    );
    assertEquals(
        "heron.binaries.sandbox.shell",
        Key.SANDBOX_SHELL_BINARY
    );
    assertEquals(
        "heron.binaries.sandbox.python.instance",
        Key.SANDBOX_PYTHON_INSTANCE_BINARY
    );
    assertEquals(
        "heron.jars.scheduler",
        Key.SCHEDULER_JAR
    );
  }

  private static void assertEquals(String value, Key key) {
    Assert.assertEquals(value, key.value());
  }
}
