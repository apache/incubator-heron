/*
 * Copyright 2016 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.heron.storage;

import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Utility used inside storage */
public class Utils {
  /** Approximately every N Secs allow the logs to be flushed down */
  public static class LogEveryNSecs {
    private volatile boolean isLogged = true;
    private Logger logger;

    /** Starts a thread which runs turns logging to true every n seconds.
     * TODO: Replace extremely frequent logging with this routine. */
    public LogEveryNSecs(Logger paramLogger, int n) {
      logger = paramLogger;
      new Timer().schedule(new TimerTask() {
        @Override
        public void run() {
          isLogged = true;
        }
      }, 0, n * 1000);
    }

    /** Will log every N seconds. Logs in between the intervals will be ignored */
    public void log(Level level, String msg) {
      if (isLogged) {
        logger.log(level, msg);
        isLogged = false;
      }
    }
  }
}
