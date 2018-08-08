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

package org.apache.heron.healthmgr.policy;

import java.time.Duration;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;

import com.microsoft.dhalion.events.EventHandler;
import com.microsoft.dhalion.events.EventManager;

import org.apache.heron.healthmgr.HealthPolicyConfig;
import org.apache.heron.healthmgr.common.HealthManagerEvents.ContainerRestart;
import org.apache.heron.healthmgr.common.PhysicalPlanProvider;
import org.apache.heron.healthmgr.detectors.BackPressureDetector;
import org.apache.heron.healthmgr.resolvers.RestartContainerResolver;
import org.apache.heron.healthmgr.sensors.BackPressureSensor;

import static org.apache.heron.healthmgr.HealthPolicyConfig.CONF_POLICY_ID;
import static org.apache.heron.healthmgr.HealthPolicyConfigReader.PolicyConfigKey.HEALTH_POLICY_INTERVAL_MS;

/**
 * This Policy class
 * 1. detector: find out the container that has been in backpressure
 * state for long time, which we believe the container cannot recover.
 * 2. resolver: try to restart the backpressure container so as to be rescheduled.
 */
public class AutoRestartBackpressureContainerPolicy extends ToggleablePolicy
    implements EventHandler<ContainerRestart> {

  private static final String CONF_WAIT_INTERVAL_MILLIS =
      "AutoRestartBackpressureContainerPolicy.conf_post_action_wait_interval_ms";
  private static final Logger LOG =
      Logger.getLogger(AutoRestartBackpressureContainerPolicy.class.getName());


  @Inject
  AutoRestartBackpressureContainerPolicy(@Named(CONF_POLICY_ID) String policyId,
                                         HealthPolicyConfig policyConfig,
                                         PhysicalPlanProvider physicalPlanProvider,
                                         EventManager eventManager,
                                         BackPressureSensor backPressureSensor,
                                         BackPressureDetector backPressureDetector,
                                         RestartContainerResolver restartContainerResolver) {
    super(policyId, policyConfig, physicalPlanProvider);

    registerSensors(backPressureSensor);
    registerDetectors(backPressureDetector);
    registerResolvers(restartContainerResolver);

    setPolicyExecutionInterval(
        Duration.ofMillis((int) policyConfig.getConfig(HEALTH_POLICY_INTERVAL_MS.key(), 60000)));

    eventManager.addEventListener(ContainerRestart.class, this);
  }

  @Override
  public void onEvent(ContainerRestart event) {
    int interval = (int) policyConfig.getConfig(CONF_WAIT_INTERVAL_MILLIS, 180000);
    LOG.info("Received container restart action event: " + event);
    setOneTimeDelay(Duration.ofMillis(interval));
  }
}
