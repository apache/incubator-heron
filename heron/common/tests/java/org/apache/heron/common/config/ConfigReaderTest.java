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

package org.apache.heron.common.config;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.commons.io.IOUtils;

public class ConfigReaderTest {
  private static final String ROLE_KEY = "role";
  private static final String ENVIRON_KEY = "environ";
  private static final String USER_KEY = "user";

  private static final String LAUNCHER_CLASS_KEY = "heron.launcher.class";

  private static final String RESOURCE_LOC = "/heron/common/tests/resources/defaults.yaml";

  private void testProperty(Map<String, Object> props) {
    Assert.assertEquals("role", props.get(ROLE_KEY));
    Assert.assertEquals("environ", props.get(ENVIRON_KEY));
    Assert.assertEquals("org.apache.heron.scheduler.aurora.AuroraLauncher",
        props.get(LAUNCHER_CLASS_KEY));
    Assert.assertNull(props.get(USER_KEY));
  }

  private InputStream loadResource() {
    InputStream inputStream  = getClass().getResourceAsStream(RESOURCE_LOC);
    if (inputStream == null) {
      throw new RuntimeException("Sample output file not found");
    }
    return inputStream;
  }

  @Test
  public void testLoadFile() throws IOException {
    InputStream inputStream = loadResource();
    File file = File.createTempFile("defaults_temp", "yaml");
    file.deleteOnExit();
    OutputStream outputStream = new FileOutputStream(file);
    IOUtils.copy(inputStream, outputStream);
    outputStream.close();
    Map<String, Object> props = ConfigReader.loadFile(file.getAbsolutePath());
    testProperty(props);
  }

  @Test
  public void testLoadStream() {
    InputStream inputStream = loadResource();
    Map<String, Object> props = ConfigReader.loadStream(inputStream);
    testProperty(props);
  }
}
