// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.scheduler.aurora;

import java.net.HttpURLConnection;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.twitter.heron.proto.system.PhysicalPlans.StMgr;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.ConfigLoader;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.statemgr.IStateManager;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.NetworkUtils;
import com.twitter.heron.spi.utils.ReflectionUtils;

/**
 * Implementation of AuroraController that delegates the `restart` container to heron-shell
 */
class AuroraHeronShellController implements AuroraController {
  private static final Logger LOG = Logger.getLogger(AuroraHeronShellController.class.getName());

  private final String topologyName;
  private SchedulerStateManagerAdaptor stateMgrAdaptor;

  AuroraHeronShellController(String jobName) {
    this.topologyName = jobName;

    stateMgrAdaptor = null;
    Config config =
        Config.toClusterMode(Config.newBuilder().putAll(ConfigLoader.loadClusterConfig()).build());
    String stateMgrClass = Context.stateManagerClass(config);
    IStateManager stateMgr = null;
    try {
      stateMgr = ReflectionUtils.newInstance(stateMgrClass);

      stateMgr.initialize(config);
      stateMgrAdaptor = new SchedulerStateManagerAdaptor(stateMgr, 5000);
    } catch (ClassNotFoundException e) {
      LOG.severe(e.getMessage());
    } catch (InstantiationException e) {
      LOG.severe(e.getMessage());
    } catch (IllegalAccessException e) {
      LOG.severe(e.getMessage());
    }
  }

  @Override
  public boolean createJob(Map<AuroraField, String> bindings) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean killJob() {
    throw new UnsupportedOperationException("Not implemented");
  }

  // Restart an aurora container
  @Override
  public boolean restart(Integer containerId) {
    if (containerId == null) {
      throw new UnsupportedOperationException("Not implemented");
    }

    if (stateMgrAdaptor == null) {
      LOG.warning("SchedulerStateManagerAdaptor not initialized");
      return false;
    }

    StMgr contaienrInfo = stateMgrAdaptor.getPhysicalPlan(topologyName).getStmgrs(containerId);
    String ip = contaienrInfo.getHostName();
    int port = contaienrInfo.getShellPort();
    String url = "http://" + ip + ":" + port + "/killexecutor";

    String payload = "secret=" + topologyName;
    LOG.info("sending `kill container` to " + url + "; payload: " + payload);

    HttpURLConnection con = NetworkUtils.getHttpConnection(url);
    NetworkUtils.sendHttpPostRequest(con, "X", payload.getBytes());
    boolean ret = NetworkUtils.checkHttpResponseCode(con, 200);
    con.disconnect();
    return ret;
  }

  @Override
  public void removeContainers(Set<PackingPlan.ContainerPlan> containersToRemove) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void addContainers(Integer count) {
    throw new UnsupportedOperationException("Not implemented");
  }
}
