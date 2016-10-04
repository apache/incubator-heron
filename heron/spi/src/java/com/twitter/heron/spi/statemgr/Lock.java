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
package com.twitter.heron.spi.statemgr;

import java.util.concurrent.TimeUnit;

/**
 * Interface for a shared lock
 */
public interface Lock {

  /**
   * Wait until timeout for a lock to be available. Return true if lock is obtained or false if it
   * can not be obtained.
   */
  boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException;

  /**
   * Release the lock. Failure to call this method could result in an orphaned lock.
   */
  void unlock();
}
