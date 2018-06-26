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

package org.apache.heron.metricsmgr.sink;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.heron.common.basics.FileUtils;
import org.apache.heron.spi.metricsmgr.sink.SinkContext;

/**
 * FileSink Tester.
 */
public class FileSinkTest {

  private FileSink fileSink;
  private File tmpDir;

  @Before
  public void before() throws IOException {
    fileSink = new FileSink();
    Map<String, Object> conf = new HashMap<>();
    tmpDir = Files.createTempDirectory("filesink").toFile();
    conf.put("filename-output", tmpDir.getAbsolutePath() + "/filesink");
    conf.put("file-maximum", 100);
    SinkContext context = Mockito.mock(SinkContext.class);
    Mockito.when(context.getMetricsMgrId()).thenReturn("test");
    fileSink.init(conf, context);
  }

  @After
  public void after() {
    fileSink.close();
    for (File file: tmpDir.listFiles()) {
      file.delete();
    }
    tmpDir.delete();
  }

  /**
   * Method: flush()
   */
  @Test
  public void testFirstFlushWithoutRecords() {
    fileSink.flush();
    String content = new String(FileUtils.readFromFile(
        new File(tmpDir, "/filesink.test.0").getAbsolutePath()), StandardCharsets.UTF_8);
    Assert.assertEquals("[]", content);
  }

  /**
   * Method: flush()
   */
  @Test
  public void testSuccessiveFlushWithoutRecords() throws UnsupportedEncodingException {
    fileSink.flush();
    fileSink.flush();
    String content = new String(FileUtils.readFromFile(
        new File(tmpDir, "/filesink.test.0").getAbsolutePath()), StandardCharsets.UTF_8);
    Assert.assertEquals("[]", content);
    content = new String(FileUtils.readFromFile(
        new File(tmpDir, "/filesink.test.1").getAbsolutePath()), StandardCharsets.UTF_8);
    Assert.assertEquals("[]", content);
  }

  /**
   * Method: init()
   */
  @Test
  public void testIllegalConf() throws IOException {
    FileSink sink = new FileSink();
    Map<String, Object> conf = new HashMap<>();
    SinkContext context = Mockito.mock(SinkContext.class);
    try {
      sink.init(conf, context);
      Assert.fail("Expected IllegalArgumentException.");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Require: filename-output", e.getMessage());
    }

    sink = new FileSink();
    conf.put("filename-output", tmpDir.getAbsolutePath() + "/filesink");
    try {
      sink.init(conf, context);
      Assert.fail("Expected IllegalArgumentException.");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Require: file-maximum", e.getMessage());
    }
  }
}
