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


package com.twitter.heron.healthmgr.policy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.microsoft.dhalion.api.IResolver;
import com.microsoft.dhalion.diagnoser.Diagnosis;
import com.microsoft.dhalion.events.EventHandler;
import com.microsoft.dhalion.events.EventManager;
import com.microsoft.dhalion.policy.HealthPolicyImpl;

import com.twitter.heron.common.basics.TypeUtils;
import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.common.HealthManagerEvents.ContainerRestart;
import com.twitter.heron.healthmgr.detectors.BackPressureDetector;
import com.twitter.heron.healthmgr.detectors.ProcessingRateSkewDetector;
import com.twitter.heron.healthmgr.detectors.WaitQueueDisparityDetector;
import com.twitter.heron.healthmgr.diagnosers.SlowInstanceDiagnoser;
import com.twitter.heron.healthmgr.resolvers.RestartContainerResolver;

import static com.twitter.heron.healthmgr.HealthPolicyConfigReader.PolicyConfigKey.HEALTH_POLICY_INTERVAL;
import static com.twitter.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisName.DIAGNOSIS_SLOW_INSTANCE;

/**
 * This Policy class
 * 1. detector: find out the container that has been in backpressure
 *              state for long time, which we believe the container cannot recover.
 * 2. resolver: try to restart the backpressure container so as to be rescheduled.
 */
public class AutoRestartBackpressureContainerPolicy extends HealthPolicyImpl
    implements EventHandler<ContainerRestart> {

  private static final String CONF_WAIT_INTERVAL_MILLIS =
      "AutoRestartBackpressureContainerPolicy.conf_post_action_wait_interval_ms";
  private static final Logger LOG =
      Logger.getLogger(AutoRestartBackpressureContainerPolicy.class.getName());

  private final HealthPolicyConfig policyConfig;
  private final RestartContainerResolver restartContainerResolver;

  @Inject
  AutoRestartBackpressureContainerPolicy(HealthPolicyConfig policyConfig, EventManager eventManager,
      BackPressureDetector backPressureDetector,
      ProcessingRateSkewDetector processingRateSkewDetector,
      WaitQueueDisparityDetector waitQueueDisparityDetector,
      SlowInstanceDiagnoser slowInstanceDiagnoser,
      RestartContainerResolver restartContainerResolver) {
    this.policyConfig = policyConfig;
    this.restartContainerResolver = restartContainerResolver;

    registerDetectors(backPressureDetector, waitQueueDisparityDetector, processingRateSkewDetector);
    registerDiagnosers(slowInstanceDiagnoser);

    setPolicyExecutionInterval(TimeUnit.MILLISECONDS,
        TypeUtils.getInteger(policyConfig.getConfig(HEALTH_POLICY_INTERVAL.key(), 60000)));

    eventManager.addEventListener(ContainerRestart.class, this);
  }

  @Override
  public IResolver selectResolver(List<Diagnosis> diagnosis) {
    Map<String, Diagnosis> diagnosisMap =
        diagnosis.stream().collect(Collectors.toMap(Diagnosis::getName, d -> d));

    if (diagnosisMap.containsKey(DIAGNOSIS_SLOW_INSTANCE.text())) {
      return restartContainerResolver;
    }

    LOG.warning("Unknown diagnoses. None resolver selected.");
    return null;
  }

  @Override
  public void onEvent(ContainerRestart event) {
    int interval = (int) policyConfig.getConfig(CONF_WAIT_INTERVAL_MILLIS, 180000);
    LOG.info("Received container restart action event: " + event);
    setOneTimeDelay(TimeUnit.MILLISECONDS, interval);
  }
}
