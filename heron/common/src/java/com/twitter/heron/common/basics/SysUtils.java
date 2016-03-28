package com.twitter.heron.common.basics;

import java.io.IOException;
import java.net.ServerSocket;

public class SysUtils {
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
   * @return available port.
   */
  public static int getFreePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      int port = socket.getLocalPort();
      socket.close();
      return port;
    } catch (IOException ioe) {
      return -1;
    }
  }
}
