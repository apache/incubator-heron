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

package org.apache.heron.spi.utils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class ShellUtilsTest {

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
    StringBuilder stderr = new StringBuilder();
    Assert.assertEquals(0, ShellUtils.runProcess("echo " + testString, stderr));
    Assert.assertEquals(testString, stderr.toString().trim());
    Assert.assertTrue(!stderr.toString().trim().isEmpty());
  }

  @Test(timeout = 60000)
  public void testLargeOutput() throws IOException, InterruptedException {
    File testScript = File.createTempFile("foo-", ".sh");
    try {
      // A command that fulfills the output buffers.
      String command = "printf '.%.0s' {1..1000000}\nprintf '.%.0s' {1..1000000} >&2";
      FileOutputStream input = new FileOutputStream(testScript);
      try {
        input.write(command.getBytes(StandardCharsets.UTF_8));
      } finally {
        input.close();
      }
      Assert.assertTrue("Cannot make the test script executable", testScript.setExecutable(true));
      StringBuilder stderr = new StringBuilder();
      Assert.assertEquals(0,
          ShellUtils.runProcess(
              "/bin/bash -c " + testScript.getAbsolutePath(), stderr));
      // Only checks stdout and stderr are not empty. Correctness is checked in "testRunProcess".
      Assert.assertTrue(!stderr.toString().trim().isEmpty());
    } finally {
      testScript.delete();
    }
  }

  @Test
  public void testRunAsyncProcess() throws IOException {
    Process p = ShellUtils.runASyncProcess("sleep 30");
    try {
      p.exitValue();
      Assert.fail(p + " was dead");
    } catch (IllegalThreadStateException e) {
      // This is expected. If the process is alive, "exitValue" will throw
      // IllegalThreadStateException
    }
    p.destroy();
  }

  @Test
  public void testRunAsyncProcessRedirectErrorStream() throws IOException {
    Process p = ShellUtils.runASyncProcess("/bin/bash blahblah.sh");
    String stdout = ShellUtils.inputstreamToString(p.getInputStream());
    // "blahblah.sh" doesn't exist so "bash" will output the error message to stderr. Since stderr
    // is redirected to stdout, stdout should contain the error message.
    Assert.assertTrue(stdout.contains("blahblah.sh: No such file or directory"));
    p.destroy();
  }

  @Test
  public void testInputstreamToString() {
    String testString = generateRandomLongString(1024);
    ByteArrayInputStream is = new ByteArrayInputStream(testString.getBytes());
    Assert.assertEquals(testString, ShellUtils.inputstreamToString(is));
  }

  @Test
  public void testGetProcessBuilder() throws IOException, InterruptedException {
    String[] command = {"printenv"};
    Map<String, String> env = new HashMap<>();
    String key = "heron-shell-utils-test-env-key";
    String value = "heron-shell-utils-test-env-value";
    env.put(key, value);
    // The process should not inherit IO
    ProcessBuilder pb = ShellUtils.getProcessBuilder(false, command, new File("."), env);

    // Process running normally
    Process p = pb.start();
    p.waitFor(2, TimeUnit.SECONDS);
    Assert.assertTrue(p.getInputStream().available() > 0);

    String output = ShellUtils.inputstreamToString(p.getInputStream());
    // Environment variables setting properly
    String entry = String.format("%s=%s", key, value);
    Assert.assertTrue(output.contains(entry));
  }
}
