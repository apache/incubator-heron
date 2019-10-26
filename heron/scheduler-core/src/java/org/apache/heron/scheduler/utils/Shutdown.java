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

package org.apache.heron.scheduler.utils;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class Shutdown {
  private static final Logger LOG = Logger.getLogger(Shutdown.class.getName());
  private final Lock lock = new ReentrantLock();
  private final Condition terminateCondition = lock.newCondition();
  private boolean terminated = false;

  public void await() {
    try {
      lock.lock();
      while (!terminated) {
        terminateCondition.await();
      }
      lock.unlock();
    } catch (InterruptedException e) {
      LOG.info("Process received interruption, terminating...");
      lock.unlock();
    }
  }

  public void terminate() {
    lock.lock();
    terminated = true;
    terminateCondition.signal();
    lock.unlock();
  }
}
