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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;

import org.junit.Assert;

public class ShellUtilsTest {

  private static final Logger LOG = Logger.getLogger(ShellUtilsTest.class.getName());

  private static void wait(int time, TimeUnit unit) {
    try {
      Thread.sleep(unit.toMillis(time));
    } catch (InterruptedException e) {
      LOG.log(Level.SEVERE, "Sleep interrupted ", e);
    }
  }

  private static String generateRandomLongString(int size) {
    StringBuilder builder = new StringBuilder();
    Random random = new Random();
    for (int i = 0; i < size; ++i) {
      builder.append('a' + random.nextInt(26));
    }
    return builder.toString();
  }

  @Test
  public void testRunProcess() {
    String testString = "testString";
    StringBuilder stdout = new StringBuilder();
    StringBuilder stderr = new StringBuilder();
    Assert.assertEquals(0, ShellUtils.runProcess(true, "echo " + testString, stdout, stderr));
    Assert.assertEquals(testString, stdout.toString().trim());
    Assert.assertTrue(stderr.toString().trim().isEmpty());
  }

  @Test
  public void testRunAsyncProcess() throws IOException {
    String testString = "testString";
    StringBuilder stdout = new StringBuilder();
    StringBuilder stderr = new StringBuilder();
    // Sleep 1 second and echo some text.
    Process p = ShellUtils.runASyncProcess(
        true, String.format("sleep 1 && echo %s", testString), new File("."));
    // Test process is running and input stream is empty
    wait(10, TimeUnit.MILLISECONDS);
    Assert.assertEquals(0, p.getInputStream().available());
  }

  @Test
  public void testInputstreamToString() {
    String testString = generateRandomLongString(1024 * 1024);  // 1 MB
    ByteArrayInputStream is = new ByteArrayInputStream(testString.getBytes());
    Assert.assertEquals(testString, ShellUtils.inputstreamToString(is));
  }
}
