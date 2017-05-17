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
import java.time.Duration;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.common.basics.ByteAmount;

public class SystemConfigTest {

  private static final String RESOURCE_LOC = "/heron/common/tests/resources/sysconfig.yaml";

  @Test
  public void testReadConfig() throws Exception {
    InputStream inputStream  = getClass().getResourceAsStream(RESOURCE_LOC);
    if (inputStream == null) {
      throw new RuntimeException("Sample output file not found");
    }
    File file = File.createTempFile("system_temp", "yaml");
    file.deleteOnExit();
    OutputStream outputStream = new FileOutputStream(file);
    IOUtils.copy(inputStream, outputStream);
    outputStream.close();
    SystemConfig systemConfig = SystemConfig.newBuilder(true)
        .putAll(file.getAbsolutePath(), true)
        .build();
    Assert.assertEquals("log-files", systemConfig.getHeronLoggingDirectory());
    Assert.assertEquals(ByteAmount.fromMegabytes(100), systemConfig.getHeronLoggingMaximumSize());
    Assert.assertEquals(5, systemConfig.getHeronLoggingMaximumFiles());
    Assert.assertEquals(Duration.ofSeconds(60), systemConfig.getHeronMetricsExportInterval());
  }
}
