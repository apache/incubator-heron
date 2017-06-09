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


package com.twitter.heron.healthmgr.policy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.dhalion.api.IResolver;
import com.microsoft.dhalion.diagnoser.Diagnosis;
import com.microsoft.dhalion.events.EventHandler;
import com.microsoft.dhalion.events.EventManager;
import com.microsoft.dhalion.policy.HealthPolicyImpl;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.common.HealthManagerEvents.TopologyUpdate;
import com.twitter.heron.healthmgr.detectors.BackPressureDetector;
import com.twitter.heron.healthmgr.diagnosers.DataSkewDiagnoser;
import com.twitter.heron.healthmgr.diagnosers.SlowInstanceDiagnoser;
import com.twitter.heron.healthmgr.diagnosers.UnderProvisioningDiagnoser;
import com.twitter.heron.healthmgr.resolvers.ScaleUpResolver;

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.HEALTH_POLICY_INTERVAL;

public class DynamicResourceAllocationPolicy extends HealthPolicyImpl
    implements EventHandler<TopologyUpdate> {

  private static final Logger LOG
      = Logger.getLogger(DynamicResourceAllocationPolicy.class.getName());

  public static final String CONF_WAIT_INTERVAL_MILLIS =
      "DynamicResourceAllocationPolicy.conf_post_action_wait_interval_min";

  private HealthPolicyConfig policyConfig;
  private ScaleUpResolver scaleUpResolver;

  @Inject
  DynamicResourceAllocationPolicy(HealthPolicyConfig policyConfig,
                                  EventManager eventManager,
                                  BackPressureDetector backPressureDetector,
                                  UnderProvisioningDiagnoser underProvisioningDiagnoser,
                                  DataSkewDiagnoser dataSkewDiagnoser,
                                  SlowInstanceDiagnoser slowInstanceDiagnoser,
                                  ScaleUpResolver scaleUpResolver) {
    this.policyConfig = policyConfig;
    this.scaleUpResolver = scaleUpResolver;

    registerDetectors(backPressureDetector);
    registerDiagnosers(underProvisioningDiagnoser, dataSkewDiagnoser, slowInstanceDiagnoser);

    setPolicyExecutionInterval(TimeUnit.MILLISECONDS,
        Long.valueOf(policyConfig.getConfig(HEALTH_POLICY_INTERVAL, "60000")));

    eventManager.addEventListener(TopologyUpdate.class, this);
  }

  @Override
  public IResolver selectResolver(List<Diagnosis> diagnosis) {
    Map<String, Diagnosis> diagnosisMap = new HashMap<>();
    for (Diagnosis diagnoses : diagnosis) {
      diagnosisMap.put(diagnoses.getName(), diagnoses);
    }

    if (diagnosisMap.containsKey(DataSkewDiagnoser.class.getName())) {
      LOG.warning("Data Skew diagnoses. This diagnosis does not have any resolver.");
    } else if (diagnosisMap.containsKey(SlowInstanceDiagnoser.class.getName())) {
      LOG.warning("Slow Instance diagnoses. This diagnosis does not have any resolver.");
    } else if (diagnosisMap.containsKey(UnderProvisioningDiagnoser.class.getSimpleName())) {
      return scaleUpResolver;
    }

    return null;
  }

  @Override
  public void onEvent(TopologyUpdate event) {
    int interval = Integer.valueOf(policyConfig.getConfig(CONF_WAIT_INTERVAL_MILLIS, "180000"));
    LOG.info("Received topology update action event: " + event);
    setOneTimeDelay(TimeUnit.MILLISECONDS, interval);
  }
}
