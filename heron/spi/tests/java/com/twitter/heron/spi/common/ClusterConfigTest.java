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

import java.nio.file.Paths;
import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.common.basics.ByteAmount;

public class ClusterConfigTest {
  private static final Logger LOG = Logger.getLogger(ClusterConfigTest.class.getName());

  private String heronHome;
  private String configPath;
  private Config basicConfig;

  @Before
  public void setUp() {
    heronHome = Paths.get(System.getenv("JAVA_RUNFILES"),
        TestConstants.TEST_DATA_PATH).toString();

    configPath = Paths.get(heronHome, "local").toString();

    basicConfig = Config.newBuilder()
        .putAll(ClusterConfig.loadHeronHome(heronHome, configPath))
        .putAll(ClusterConfig.loadConfigHome(heronHome, configPath))
        .build();
  }

  /**
   * Test reading the cluster.yaml file
   */
  @Test
  public void testClusterFile() throws Exception {

    Config props = ClusterConfig.loadConfig(Context.clusterFile(basicConfig));

    Assert.assertEquals(4, props.size());

    Assert.assertEquals(
        "com.twitter.heron.uploader.localfs.FileSystemUploader",
        Context.uploaderClass(props)
    );

  }

  /**
   * Test reading the defaults.yaml file
   */
  @Test
  public void testDefaultsFile() throws Exception {
    Config props = ClusterConfig.loadConfig(Context.defaultsFile(basicConfig));

    Assert.assertEquals(11, props.size());

    Assert.assertEquals(
        "heron-executor",
        Context.executorSandboxBinary(props)
    );

    Assert.assertEquals(
        "heron-stmgr",
        Context.stmgrSandboxBinary(props)
    );

    Assert.assertEquals(
        "heron-tmaster",
        Context.tmasterSandboxBinary(props)
    );

    Assert.assertEquals(
        "heron-shell",
        Context.shellSandboxBinary(props)
    );

    Assert.assertEquals(
        "heron-python-instance",
        Context.pythonInstanceSandboxBinary(props)
    );

    Assert.assertEquals(
        "heron-scheduler.jar",
        Context.schedulerJar(props)
    );

    Assert.assertEquals(
        Double.valueOf(1),
        Context.instanceCpu(props),
        0.001
    );

    Assert.assertEquals(
        ByteAmount.fromMegabytes(128),
        Context.instanceRam(props)
    );

    Assert.assertEquals(
        ByteAmount.fromMegabytes(256),
        Context.instanceDisk(props)
    );

    Assert.assertEquals(
        ByteAmount.fromMegabytes(512),
        Context.stmgrRam(props)
    );
  }

  @Test
  public void testSchedulerFile() throws Exception {
    Config props = ClusterConfig.loadConfig(Context.schedulerFile(basicConfig));

    Assert.assertEquals(2, props.size());

    Assert.assertEquals(
        "com.twitter.heron.scheduler.local.LocalScheduler",
        Context.schedulerClass(props)
    );

    Assert.assertEquals(
        "com.twitter.heron.scheduler.local.LocalLauncher",
        Context.launcherClass(props)
    );
  }

  @Test
  public void testPackingFile() throws Exception {
    Config props = ClusterConfig.loadConfig(Context.packingFile(basicConfig));

    Assert.assertEquals(1, props.size());
    Assert.assertEquals(
        "com.twitter.heron.packing.roundrobin.RoundRobinPacking",
        props.getStringValue("heron.class.packing.algorithm")
    );
  }

  @Test
  public void testUploaderFile() throws Exception {
    Config props = ClusterConfig.loadConfig(Context.uploaderFile(basicConfig));

    Assert.assertEquals(2, props.size());
    Assert.assertEquals(
        "/vagrant/heron/jobs",
        props.getStringValue("heron.uploader.file.system.path")
    );
  }
}
