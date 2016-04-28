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

public class ConfigReaderTest {
  private static final Logger LOG = Logger.getLogger(ConfigReaderTest.class.getName());

  @Test
  public void testLoadFile() throws Exception {
    String file = Paths.get(System.getenv("JAVA_RUNFILES"),
        Constants.TEST_DATA_PATH, "defaults.yaml").toString();
    Map<String, Object> props = ConfigReader.loadFile(file);

    Assert.assertEquals("role", props.get(Constants.ROLE_KEY));
    Assert.assertEquals("environ", props.get(Constants.ENVIRON_KEY));
    Assert.assertEquals("com.twitter.heron.scheduler.aurora.AuroraLauncher",
        props.get(Constants.LAUNCHER_CLASS_KEY));

    Assert.assertNull(props.get(Constants.USER_KEY));
  }
}
