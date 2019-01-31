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

package org.apache.heron.scheduler.aurora;

import java.net.HttpURLConnection;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.heron.proto.system.PhysicalPlans.StMgr;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.statemgr.IStateManager;
import org.apache.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import org.apache.heron.spi.utils.NetworkUtils;
import org.apache.heron.spi.utils.ReflectionUtils;

/**
 * Implementation of AuroraController that is a wrapper of AuroraCLIController.
 * The difference is `restart` command:
 * 1. restart whole topology: delegate to AuroraCLIController
 * 2. restart container 0: delegate to AuroraCLIController
 * 3. restart container x(x>0): call heron-shell endpoint `/killexecutor`
 * For backpressure, only containers with heron-stmgr may send out backpressure.
 * This class is to handle `restart backpressure containers inside container`,
 * while delegating to AuroraCLIController for all the other scenarios.
 */
class AuroraHeronShellController implements AuroraController {
  private static final Logger LOG = Logger.getLogger(AuroraHeronShellController.class.getName());

  private final String topologyName;
  private final AuroraCLIController cliController;
  private final SchedulerStateManagerAdaptor stateMgrAdaptor;

  AuroraHeronShellController(String jobName, String cluster, String role, String env,
      String auroraFilename, boolean isVerbose, Config localConfig)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    this.topologyName = jobName;
    this.cliController =
        new AuroraCLIController(jobName, cluster, role, env, auroraFilename, isVerbose);

    Config config = Config.toClusterMode(localConfig);
    String stateMgrClass = Context.stateManagerClass(config);
    IStateManager stateMgr = ReflectionUtils.newInstance(stateMgrClass);
    stateMgr.initialize(config);
    stateMgrAdaptor = new SchedulerStateManagerAdaptor(stateMgr, 5000);
  }

  @Override
  public boolean createJob(Map<AuroraField, String> bindings, Map<String, String> extra) {
    return cliController.createJob(bindings, extra);
  }

  @Override
  public boolean killJob() {
    return cliController.killJob();
  }

  private StMgr searchContainer(Integer id) {
    String prefix = "stmgr-" + id;
    for (StMgr sm : stateMgrAdaptor.getPhysicalPlan(topologyName).getStmgrsList()) {
      if (sm.getId().equals(prefix)) {
        return sm;
      }
    }
    return null;
  }

  // Restart an aurora container
  @Override
  public boolean restart(Integer containerId) {
    // there is no backpressure for container 0, delegate to aurora client
    if (containerId == null || containerId == 0) {
      return cliController.restart(containerId);
    }

    if (stateMgrAdaptor == null) {
      LOG.warning("SchedulerStateManagerAdaptor not initialized");
      return false;
    }

    StMgr sm = searchContainer(containerId);
    if (sm == null) {
      LOG.warning("container not found in pplan " + containerId);
      return false;
    }

    String url = "http://" + sm.getHostName() + ":" + sm.getShellPort() + "/killexecutor";
    String payload = "secret=" + stateMgrAdaptor.getExecutionState(topologyName).getTopologyId();
    LOG.info("sending `kill container` to " + url + "; payload: " + payload);

    HttpURLConnection con = NetworkUtils.getHttpConnection(url);
    try {
      if (NetworkUtils.sendHttpPostRequest(con, "X", payload.getBytes())) {
        return NetworkUtils.checkHttpResponseCode(con, 200);
      } else { // if heron-shell command fails, delegate to aurora client
        LOG.info("heron-shell killexecutor failed; try aurora client ..");
        return cliController.restart(containerId);
      }
    } finally {
      con.disconnect();
    }
  }

  @Override
  public void removeContainers(Set<PackingPlan.ContainerPlan> containersToRemove) {
    cliController.removeContainers(containersToRemove);
  }

  @Override
  public Set<Integer> addContainers(Integer count) {
    return cliController.addContainers(count);
  }
}
