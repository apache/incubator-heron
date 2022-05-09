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

import org.junit.Assert;
import org.junit.Test;

public class KeysTest {

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
         "heron.config.verbose_gc",
         Key.VERBOSE_GC
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
        "heron.binaries.executor",
        Key.EXECUTOR_BINARY
    );
    assertEquals(
        "heron.binaries.stmgr",
        Key.STMGR_BINARY
    );
    assertEquals(
        "heron.binaries.tmanager",
        Key.TMANAGER_BINARY
    );
    assertEquals(
        "heron.binaries.shell",
        Key.SHELL_BINARY
    );
    assertEquals(
        "heron.binaries.python.instance",
        Key.PYTHON_INSTANCE_BINARY
    );
    assertEquals(
        "heron.binaries.cpp.instance",
        Key.CPP_INSTANCE_BINARY
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
