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

package org.apache.heron.scheduler.client;

import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.scheduler.IScheduler;

/**
 * This class manages topology by invoking IScheduler's interface directly as a library.
 */
public class LibrarySchedulerClient implements ISchedulerClient {
  private final Config config;
  private final Config runtime;
  private final IScheduler scheduler;

  public LibrarySchedulerClient(Config config, Config runtime, IScheduler scheduler) {
    this.config = config;
    this.runtime = runtime;
    this.scheduler = scheduler;
  }

  @Override
  public boolean restartTopology(Scheduler.RestartTopologyRequest restartTopologyRequest) {
    boolean ret = false;

    try {
      scheduler.initialize(config, runtime);
      ret = scheduler.onRestart(restartTopologyRequest);
    } finally {
      SysUtils.closeIgnoringExceptions(scheduler);
    }

    return ret;
  }

  @Override
  public boolean killTopology(Scheduler.KillTopologyRequest killTopologyRequest) {
    boolean ret = false;

    try {
      scheduler.initialize(config, runtime);
      ret = scheduler.onKill(killTopologyRequest);
    } finally {
      SysUtils.closeIgnoringExceptions(scheduler);
    }

    return ret;
  }

  @Override
  public boolean updateTopology(Scheduler.UpdateTopologyRequest updateTopologyRequest) {
    boolean ret = false;

    try {
      scheduler.initialize(config, runtime);
      ret = scheduler.onUpdate(updateTopologyRequest);
    } finally {
      SysUtils.closeIgnoringExceptions(scheduler);
    }

    return ret;
  }

  // TODO(mfu): Use JAVA8's lambda feature providing a method for all commands in SchedulerUtils
  // TODO(mfu): boolean invokeSchedulerAsLibrary(String commandName, Function invoker);
}
