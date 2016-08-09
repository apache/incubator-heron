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

import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Test;

public class ConfigKeysTest {
  private static final Logger LOG = Logger.getLogger(ConfigKeysTest.class.getName());

  @Test
  public void testHeronEnviron() throws Exception {
    Assert.assertEquals(
        "heron.directory.home",
        ConfigKeys.get("HERON_HOME")
    );
    Assert.assertEquals(
        "heron.directory.bin",
        ConfigKeys.get("HERON_BIN")
    );
    Assert.assertEquals(
        "heron.directory.conf",
        ConfigKeys.get("HERON_CONF")
    );
    Assert.assertEquals(
        "heron.directory.lib",
        ConfigKeys.get("HERON_LIB")
    );
    Assert.assertEquals(
        "heron.directory.dist",
        ConfigKeys.get("HERON_DIST")
    );
    Assert.assertEquals(
        "heron.directory.etc",
        ConfigKeys.get("HERON_ETC")
    );
    Assert.assertEquals(
        "heron.directory.java.home",
        ConfigKeys.get("JAVA_HOME")
    );
  }

  @Test
  public void testConfigFiles() throws Exception {
    Assert.assertEquals(
        "heron.config.file.cluster.yaml",
        ConfigKeys.get("CLUSTER_YAML")
    );
    Assert.assertEquals(
        "heron.config.file.defaults.yaml",
        ConfigKeys.get("DEFAULTS_YAML")
    );
    Assert.assertEquals(
        "heron.config.file.metrics.yaml",
        ConfigKeys.get("METRICS_YAML")
    );
    Assert.assertEquals(
        "heron.config.file.packing.yaml",
        ConfigKeys.get("PACKING_YAML")
    );
    Assert.assertEquals(
        "heron.config.file.scheduler.yaml",
        ConfigKeys.get("SCHEDULER_YAML")
    );
    Assert.assertEquals(
        "heron.config.file.statemgr.yaml",
        ConfigKeys.get("STATEMGR_YAML")
    );
    Assert.assertEquals(
        "heron.config.file.system.yaml",
        ConfigKeys.get("SYSTEM_YAML")
    );
    Assert.assertEquals(
        "heron.config.file.uploader.yaml",
        ConfigKeys.get("UPLOADER_YAML")
    );
  }

  @Test
  public void testConfig() throws Exception {
    Assert.assertEquals(
        "heron.config.cluster",
        ConfigKeys.get("CLUSTER")
    );
    Assert.assertEquals(
        "heron.config.role",
        ConfigKeys.get("ROLE")
    );
    Assert.assertEquals(
        "heron.config.environ",
        ConfigKeys.get("ENVIRON")
    );
    Assert.assertEquals(
        "heron.config.verbose",
        ConfigKeys.get("VERBOSE")
    );
    Assert.assertEquals(
        "heron.config.path",
        ConfigKeys.get("CONFIG_PATH")
    );
    Assert.assertEquals(
        "heron.config.property",
        ConfigKeys.get("CONFIG_PROPERTY")
    );
  }

  @Test
  public void testConfigClasses() throws Exception {
    Assert.assertEquals(
        "heron.class.uploader",
        ConfigKeys.get("UPLOADER_CLASS")
    );
    Assert.assertEquals(
        "heron.class.launcher",
        ConfigKeys.get("LAUNCHER_CLASS")
    );
    Assert.assertEquals(
        "heron.class.scheduler",
        ConfigKeys.get("SCHEDULER_CLASS")
    );
    Assert.assertEquals(
        "heron.class.packing.algorithm",
        ConfigKeys.get("PACKING_CLASS")
    );
    Assert.assertEquals(
        "heron.class.state.manager",
        ConfigKeys.get("STATE_MANAGER_CLASS")
    );
  }

  @Test
  public void testBinaries() throws Exception {
    Assert.assertEquals(
        "heron.binaries.sandbox.executor",
        ConfigKeys.get("SANDBOX_EXECUTOR_BINARY")
    );
    Assert.assertEquals(
        "heron.binaries.sandbox.stmgr",
        ConfigKeys.get("SANDBOX_STMGR_BINARY")
    );
    Assert.assertEquals(
        "heron.binaries.sandbox.tmaster",
        ConfigKeys.get("SANDBOX_TMASTER_BINARY")
    );
    Assert.assertEquals(
        "heron.binaries.sandbox.shell",
        ConfigKeys.get("SANDBOX_SHELL_BINARY")
    );
    Assert.assertEquals(
        "heron.binaries.sandbox.python.instance",
        ConfigKeys.get("SANDBOX_PYTHON_INSTANCE_BINARY")
    );
    Assert.assertEquals(
        "heron.jars.scheduler",
        ConfigKeys.get("SCHEDULER_JAR")
    );
  }
}
