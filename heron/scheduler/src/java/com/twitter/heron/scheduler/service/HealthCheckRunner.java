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

package com.twitter.heron.scheduler.service;

import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.scheduler.util.NetworkUtility;

import java.util.concurrent.TimeUnit;

/**
 * Performs health check on scheduler.
 */
public class HealthCheckRunner implements Runnable {
  private final IScheduler scheduler;

  public HealthCheckRunner(IScheduler scheduler) {
    this.scheduler = scheduler;
  }

  public void run() {
    // TODO(nbhagat): Implement health-check to executor.
    NetworkUtility.await(1, TimeUnit.MILLISECONDS);
  }
}
