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

package org.apache.heron.apiserver.utils;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.heron.common.utils.logging.LoggingHelper;

public final class Logging {

  private static final String DEFAULT_FORMAT = "[%1$tF %1$tT %1$tz] [%4$s] %3$s: %5$s %6$s %n";

  private static volatile boolean isVerbose;

  public static void setVerbose(boolean verbose) {
    isVerbose = verbose;
  }

  public static boolean isVerbose() {
    return isVerbose;
  }

  public static void configure(boolean verbose) {
    LoggingHelper.setLoggingFormat(DEFAULT_FORMAT);

    final Level level = verbose ? Level.ALL : Level.INFO;
    // configure the root heron logger and it's handlers
    Logger logger = Logger.getLogger("");
    for (Handler handler : logger.getHandlers()) {
      handler.setLevel(level);
    }

    logger.setLevel(level);
  }

  private Logging() {
  }
}
