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

import java.net.ServerSocket;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;


public class SysUtilsTest {
  @Test
  public void testFreePort() throws Exception {
    int numAttempts = 100;
    // Randomized test
    for (int i = 0; i < numAttempts; ++i) {
      int port = SysUtils.getFreePort();

      // verify that port is free
      new ServerSocket(port).close();
    }
  }

  @Test
  public void testSleep() throws Exception {
    long expectedSleepTimeMs = new Random().nextLong() % 1000;
    long start = System.currentTimeMillis();
    SysUtils.sleep(expectedSleepTimeMs);
    long end = System.currentTimeMillis();
    Assert.assertTrue((end - start) >= expectedSleepTimeMs);
  }
}
