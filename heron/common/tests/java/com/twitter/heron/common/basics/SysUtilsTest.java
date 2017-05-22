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

package com.twitter.heron.common.basics;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;


public class SysUtilsTest {
  public static final int NUM_ATTEMPTS = 100;

  @Test
  public void testFreePort() throws Exception {
    // Randomized test
    for (int i = 0; i < NUM_ATTEMPTS; ++i) {
      int port = SysUtils.getFreePort();

      // verify that port is free
      new ServerSocket(port).close();
    }
  }

  @Test
  public void testSleep() throws Exception {
    for (int i = 0; i < NUM_ATTEMPTS; i++) {
      // The value can not be negative
      Duration expectedSleepTime = Duration.ofMillis(new Random().nextInt(100));
      long start = System.currentTimeMillis();
      SysUtils.sleep(expectedSleepTime);
      long end = System.currentTimeMillis();
      Assert.assertTrue((end - start) >= expectedSleepTime.toMillis());
    }
  }

  @Test
  public void testCloseIgnoringException() throws Exception {
    Closeable closeable = new Closeable() {
      @Override
      public void close() throws IOException {
        throw new RuntimeException("Deliberate exception to be ignored.");
      }
    };

    // This invocation should not throw any exceptions
    // Otherwise, the test will automatically fail
    SysUtils.closeIgnoringExceptions(closeable);
    Assert.assertTrue(true);
  }
}
