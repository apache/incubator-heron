package com.twitter.heron.spi.common;

import junit.framework.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.lang.String;
import java.lang.StringBuilder;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import java.util.logging.Level;
import java.util.logging.Logger;

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
