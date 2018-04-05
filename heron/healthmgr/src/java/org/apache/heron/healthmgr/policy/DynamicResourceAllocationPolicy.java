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


package org.apache.heron.healthmgr.policy;

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

import org.apache.heron.healthmgr.HealthPolicyConfig;
import org.apache.heron.healthmgr.common.HealthManagerEvents.TopologyUpdate;
import org.apache.heron.healthmgr.detectors.BackPressureDetector;
import org.apache.heron.healthmgr.detectors.LargeWaitQueueDetector;
import org.apache.heron.healthmgr.detectors.ProcessingRateSkewDetector;
import org.apache.heron.healthmgr.detectors.WaitQueueDisparityDetector;
import org.apache.heron.healthmgr.diagnosers.DataSkewDiagnoser;
import org.apache.heron.healthmgr.diagnosers.SlowInstanceDiagnoser;
import org.apache.heron.healthmgr.diagnosers.UnderProvisioningDiagnoser;
import org.apache.heron.healthmgr.resolvers.ScaleUpResolver;

import static org.apache.heron.healthmgr.HealthPolicyConfigReader.PolicyConfigKey.HEALTH_POLICY_INTERVAL;
import static org.apache.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisName.DIAGNOSIS_DATA_SKEW;
import static org.apache.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisName.DIAGNOSIS_SLOW_INSTANCE;
import static org.apache.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisName.DIAGNOSIS_UNDER_PROVISIONING;

public class DynamicResourceAllocationPolicy extends HealthPolicyImpl
    implements EventHandler<TopologyUpdate> {

  private static final String CONF_WAIT_INTERVAL_MILLIS =
      "DynamicResourceAllocationPolicy.conf_post_action_wait_interval_min";

  private static final Logger LOG
      = Logger.getLogger(DynamicResourceAllocationPolicy.class.getName());
  private HealthPolicyConfig policyConfig;
  private ScaleUpResolver scaleUpResolver;

  @Inject
  DynamicResourceAllocationPolicy(HealthPolicyConfig policyConfig,
                                  EventManager eventManager,
                                  BackPressureDetector backPressureDetector,
                                  LargeWaitQueueDetector largeWaitQueueDetector,
                                  ProcessingRateSkewDetector dataSkewDetector,
                                  WaitQueueDisparityDetector waitQueueDisparityDetector,
                                  UnderProvisioningDiagnoser underProvisioningDiagnoser,
                                  DataSkewDiagnoser dataSkewDiagnoser,
                                  SlowInstanceDiagnoser slowInstanceDiagnoser,
                                  ScaleUpResolver scaleUpResolver) {
    this.policyConfig = policyConfig;
    this.scaleUpResolver = scaleUpResolver;

    registerDetectors(backPressureDetector, largeWaitQueueDetector,
        waitQueueDisparityDetector, dataSkewDetector);
    registerDiagnosers(underProvisioningDiagnoser, dataSkewDiagnoser, slowInstanceDiagnoser);

    setPolicyExecutionInterval(TimeUnit.MILLISECONDS,
        (int) policyConfig.getConfig(HEALTH_POLICY_INTERVAL.key(), 60000));

    eventManager.addEventListener(TopologyUpdate.class, this);
  }

  @Override
  public IResolver selectResolver(List<Diagnosis> diagnosis) {
    Map<String, Diagnosis> diagnosisMap
        = diagnosis.stream().collect(Collectors.toMap(Diagnosis::getName, d -> d));

    if (diagnosisMap.containsKey(DIAGNOSIS_DATA_SKEW.text())) {
      LOG.warning("Data Skew diagnoses. This diagnosis does not have any resolver.");
    } else if (diagnosisMap.containsKey(DIAGNOSIS_SLOW_INSTANCE.text())) {
      LOG.warning("Slow Instance diagnoses. This diagnosis does not have any resolver.");
    } else if (diagnosisMap.containsKey(DIAGNOSIS_UNDER_PROVISIONING.text())) {
      return scaleUpResolver;
    }

    return null;
  }

  @Override
  public void onEvent(TopologyUpdate event) {
    int interval = (int) policyConfig.getConfig(CONF_WAIT_INTERVAL_MILLIS, 180000);
    LOG.info("Received topology update action event: " + event);
    setOneTimeDelay(TimeUnit.MILLISECONDS, interval);
  }
}
