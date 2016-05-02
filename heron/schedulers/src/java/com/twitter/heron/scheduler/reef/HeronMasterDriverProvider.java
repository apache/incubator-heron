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

package com.twitter.heron.scheduler.reef;

import java.util.Optional;

import com.twitter.heron.scheduler.SchedulerMain;

/**
 * {@link HeronMasterDriverProvider} is used by {@link ReefScheduler} to discover and use
 * instance of {@link HeronMasterDriver} injected by REEF framework
 */
public final class HeronMasterDriverProvider {

  /**
   * Heron-REEF scheduler is initialized by {@link SchedulerMain}. This instance of
   * {@link HeronMasterDriver} is needed by the scheduler to manage the containers.
   */
  private static Optional<HeronMasterDriver> instance;

  /**
   * This is a utility class and should not be instantiated.
   */
  private HeronMasterDriverProvider() {
  }

  public static HeronMasterDriver getInstance() {
    return instance.get();
  }

  static void setInstance(HeronMasterDriver instance) {
    if (HeronMasterDriverProvider.instance != null) {
      throw new RuntimeException("Resetting Heron Driver instance is not allowed");
    }

    HeronMasterDriverProvider.instance = Optional.of(instance);
  }
}
