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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClusterConfigTest {
  private static final String TEST_DATA_PATH =
      "/__main__/heron/spi/tests/java/com/twitter/heron/spi/common/testdata";

  private Config basicConfig;

  @Before
  public void setUp() {
    String heronHome = Paths.get(System.getenv("JAVA_RUNFILES"), TEST_DATA_PATH).toString();
    String configPath = Paths.get(heronHome, "local").toString();

    basicConfig = ClusterConfig.loadConfig(heronHome, configPath, null, null);
  }

  /**
   * Test reading the cluster.yaml file
   */
  @Test
  public void testClusterFile() throws Exception {

    Config props = ClusterConfig.loadConfig(Context.clusterFile(basicConfig));

    assertEquals(4, props.size());
    assertEquals(
        "com.twitter.heron.uploader.localfs.FileSystemUploader",
        Context.uploaderClass(props)
    );

  }

  @Test
  public void testSchedulerFile() throws Exception {
    Config props = ClusterConfig.loadConfig(Context.schedulerFile(basicConfig));

    assertEquals(2, props.size());
    assertEquals(
        "com.twitter.heron.scheduler.local.LocalScheduler",
        Context.schedulerClass(props)
    );

    assertEquals(
        "com.twitter.heron.scheduler.local.LocalLauncher",
        Context.launcherClass(props)
    );
  }

  @Test
  public void testPackingFile() throws Exception {
    Config props = ClusterConfig.loadConfig(Context.packingFile(basicConfig));

    assertEquals(1, props.size());
    assertEquals(
        "com.twitter.heron.packing.roundrobin.RoundRobinPacking",
        props.getStringValue("heron.class.packing.algorithm")
    );
  }

  @Test
  public void testUploaderFile() throws Exception {
    Config props = ClusterConfig.loadConfig(Context.uploaderFile(basicConfig));

    assertEquals(2, props.size());
    assertEquals(
        "/vagrant/heron/jobs",
        props.getStringValue("heron.uploader.file.system.path")
    );
  }
}
