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
import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class SysUtils {
  private static final Logger LOG = Logger.getLogger(SysUtils.class.getName());

  private SysUtils() {
  }

  /**
   * Causes the currently executing thread to sleep (temporarily cease
   * execution) for the specified duration, subject to
   * the precision and accuracy of system timers and schedulers. The thread
   * does not lose ownership of any monitors.
   *
   * @param duration the length of time to sleep
   * @throws IllegalArgumentException if the value of {@code millis} is negative
   */
  public static void sleep(Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
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
      return socket.getLocalPort();
    } catch (IOException ioe) {
      return -1;
    }
  }

  /**
   * Close a closable ignoring any exceptions.
   * This method is used during cleanup, or in a finally block.
   *
   * @param closeable source or destination of data can be closed
   */
  public static void closeIgnoringExceptions(AutoCloseable closeable) {
    if (closeable != null) {
      try {
        closeable.close();

        // Suppress it since we ignore any exceptions
        // SUPPRESS CHECKSTYLE IllegalCatch
      } catch (Exception e) {
        // Still log the Exception for issue tracing
        LOG.log(Level.WARNING, String.format("Failed to close %s", closeable), e);
      }
    }
  }
}
