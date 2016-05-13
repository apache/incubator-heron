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

import java.io.IOException;
import java.net.ServerSocket;

public final class SysUtils {

  private SysUtils() {
  }

  /**
   * Causes the currently executing thread to sleep (temporarily cease
   * execution) for the specified number of milliseconds, subject to
   * the precision and accuracy of system timers and schedulers. The thread
   * does not lose ownership of any monitors.
   *
   * @param millis the length of time to sleep in milliseconds
   * @throws IllegalArgumentException if the value of {@code millis} is negative
   */
  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get available port.
   *
   * @return available port. -1 if no available ports exist
   */
  public static int getFreePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      int port = socket.getLocalPort();
      return port;
    } catch (IOException ioe) {
      return -1;
    }
  }
}
