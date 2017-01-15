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

package com.twitter.heron.common.config;

import java.nio.file.Paths;
import java.util.Map;
import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Test;

public class ClusterConfigReaderTest {
  private static final Logger LOG = Logger.getLogger(ClusterConfigReaderTest.class.getName());

  @Test
  public void testLoadClusterAndDefaultFiles() throws Exception {
    String configPath = Paths.get(System.getenv("JAVA_RUNFILES"),
        Constants.TEST_DATA_PATH).toString();
    Map<String, Object> props = ClusterConfigReader.load("cluster", configPath, "defaults.yaml");
    Assert.assertTrue(!props.isEmpty());

    Assert.assertEquals("role1", props.get(Constants.ROLE_KEY));
    Assert.assertEquals("environ", props.get(Constants.ENVIRON_KEY));
    Assert.assertEquals("user1", props.get(Constants.USER_KEY));
    Assert.assertEquals("com.twitter.heron.scheduler.local.LocalLauncher",
        props.get(Constants.LAUNCHER_CLASS_KEY));

    Assert.assertNull(props.get(Constants.VERSION_KEY));
  }

  @Test
  public void testLoadDefaultFile() throws Exception {
    String configPath = Paths.get(System.getenv("JAVA_RUNFILES"),
        Constants.TEST_DATA_PATH).toString();
    Map<String, Object> props = ClusterConfigReader.load("cluster", configPath, "defaults1.yaml");
    Assert.assertTrue(!props.isEmpty());

    Assert.assertEquals("role", props.get(Constants.ROLE_KEY));
    Assert.assertEquals("environ", props.get(Constants.ENVIRON_KEY));
    Assert.assertEquals("group", props.get(Constants.GROUP_KEY));
    Assert.assertEquals("com.twitter.heron.scheduler.aurora.AuroraLauncher",
        props.get(Constants.LAUNCHER_CLASS_KEY));

    Assert.assertNull(props.get(Constants.USER_KEY));
    Assert.assertNull(props.get(Constants.VERSION_KEY));
  }

  @Test
  public void testLoadClusterFile() throws Exception {
    String configPath = Paths.get(System.getenv("JAVA_RUNFILES"),
        Constants.TEST_DATA_PATH).toString();
    Map<String, Object> props = ClusterConfigReader.load("cluster", configPath, "defaults2.yaml");
    Assert.assertTrue(!props.isEmpty());

    Assert.assertEquals("role1", props.get(Constants.ROLE_KEY));
    Assert.assertEquals("10.1", props.get(Constants.VERSION_KEY).toString());
    Assert.assertEquals("com.twitter.heron.scheduler.local.LocalLauncher",
        props.get(Constants.LAUNCHER_CLASS_KEY));

    Assert.assertNull(props.get(Constants.ENVIRON_KEY));
    Assert.assertNull(props.get(Constants.GROUP_KEY));
  }
}
