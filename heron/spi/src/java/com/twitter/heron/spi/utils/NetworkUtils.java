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

package com.twitter.heron.spi.utils;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.logging.Logger;

import com.twitter.heron.proto.system.Common;

/**
 * Utilities related to network.
 */
public class NetworkUtils {
  private static final Logger LOG = Logger.getLogger(NetworkUtils.class.getName());

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

  public static Common.Status getHeronStatus(boolean isOK) {
    Common.Status.Builder status = Common.Status.newBuilder();
    if (isOK) {
      status.setStatus(Common.StatusCode.OK);
    } else {
      status.setStatus(Common.StatusCode.NOTOK);
    }
    return status.build();
  }
}
