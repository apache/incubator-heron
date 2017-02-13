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

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

public class ConfigReaderTest {

  private void testProperty(Map<String, Object> props) {
    Assert.assertEquals("role", props.get(Constants.ROLE_KEY));
    Assert.assertEquals("environ", props.get(Constants.ENVIRON_KEY));
    Assert.assertEquals("com.twitter.heron.scheduler.aurora.AuroraLauncher",
        props.get(Constants.LAUNCHER_CLASS_KEY));
    Assert.assertNull(props.get(Constants.USER_KEY));
  }

  private InputStream loadResource() {
    final String RESOURCE_LOC = "/heron/common/tests/resources/defaults.yaml";
    InputStream inputStream  = ConfigReaderTest.class.
        getResourceAsStream(RESOURCE_LOC);
    if (inputStream == null) {
      throw new RuntimeException("Sample output file not found");
    }
    return inputStream;
  }

  @Test
  public void testLoadFile() throws Exception {
    InputStream inputStream = loadResource();
    final String PREFIX = "defaults_temp";
    final String SUFFIX = "yaml";
    File file = File.createTempFile(PREFIX, SUFFIX);
    file.deleteOnExit();
    OutputStream outputStream = new FileOutputStream(file);
    IOUtils.copy(inputStream, outputStream);
    outputStream.close();
    Map<String, Object> props =
        ConfigReader.loadFile(file.getAbsolutePath());
    testProperty(props);
  }

  @Test
  public void testLoadStream() throws Exception {
    InputStream inputStream = loadResource();
    Map<String, Object> props =
        ConfigReader.loadStream(inputStream);
    testProperty(props);
  }
}
