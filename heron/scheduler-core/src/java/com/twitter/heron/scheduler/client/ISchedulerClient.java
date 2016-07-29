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

package com.twitter.heron.scheduler.client;

import com.twitter.heron.proto.scheduler.Scheduler;

/**
 * Implementing this interface allows an object to manage topology via scheduler
 */
public interface ISchedulerClient {
  /**
   * Restart a topology on given RestartTopologyRequest
   *
   * @param restartTopologyRequest info for restart command
   * @return true if restarted successfully
   */
  boolean restartTopology(Scheduler.RestartTopologyRequest restartTopologyRequest);

  /**
   * Kill a topology on given KillTopologyRequest
   *
   * @param killTopologyRequest info for kill command
   * @return true if killed successfully
   */
  boolean killTopology(Scheduler.KillTopologyRequest killTopologyRequest);

  /**
   * Update a topology on given UpdateTopologyRequest
   *
   * @param updateTopologyRequest info for update command
   * @return true if updated successfully
   */
  boolean updateTopology(Scheduler.UpdateTopologyRequest updateTopologyRequest);
}
