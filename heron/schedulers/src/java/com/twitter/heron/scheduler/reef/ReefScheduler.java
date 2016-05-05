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

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.proto.scheduler.Scheduler.KillTopologyRequest;
import com.twitter.heron.proto.scheduler.Scheduler.RestartTopologyRequest;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.scheduler.IScheduler;

/**
 * {@link ReefScheduler} in invoked by Heron Scheduler to perform topology actions on REEF
 * cluster. This instance will delegate all topology management functions to
 * {@link HeronMasterDriver}.
 */
public class ReefScheduler implements IScheduler {
  private static final Logger LOG = Logger.getLogger(ReefScheduler.class.getName());

  @Override
  public void initialize(Config config, Config runtime) {
  }

  @Override
  public boolean onSchedule(PackingPlan packing) {
    LOG.log(Level.INFO, "Launching topology master for packing: {0}", packing.id);
    HeronMasterDriver driver = HeronMasterDriverProvider.getInstance();
    driver.scheduleTMasterContainer();
    driver.scheduleHeronWorkers(packing);
    return true;
  }

  @Override
  public List<String> getJobLinks() {
    // TODO need to add this implementation
    return null;
  }

  @Override
  public boolean onKill(KillTopologyRequest request) {
    HeronMasterDriverProvider.getInstance().killTopology();
    return true;
  }

  @Override
  public boolean onRestart(RestartTopologyRequest request) {
    // TODO: Need to provide this implementation
    return false;
  }

  @Override
  public void close() {
    //TODO
  }
}
