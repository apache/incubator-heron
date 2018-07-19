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
import java.util.Collection;
import java.util.Collections;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.dhalion.core.Action;
import com.microsoft.dhalion.core.Diagnosis;
import com.microsoft.dhalion.core.DiagnosisTable;
import com.microsoft.dhalion.events.EventHandler;
import com.microsoft.dhalion.events.EventManager;
import com.microsoft.dhalion.policy.HealthPolicyImpl;

import org.apache.heron.healthmgr.HealthPolicyConfig;
import org.apache.heron.healthmgr.common.HealthManagerEvents.TopologyUpdate;
import org.apache.heron.healthmgr.detectors.BackPressureDetector;
import org.apache.heron.healthmgr.detectors.LargeWaitQueueDetector;
import org.apache.heron.healthmgr.detectors.ProcessingRateSkewDetector;
import org.apache.heron.healthmgr.detectors.WaitQueueSkewDetector;
import org.apache.heron.healthmgr.diagnosers.DataSkewDiagnoser;
import org.apache.heron.healthmgr.diagnosers.SlowInstanceDiagnoser;
import org.apache.heron.healthmgr.diagnosers.UnderProvisioningDiagnoser;
import org.apache.heron.healthmgr.resolvers.ScaleUpResolver;
import org.apache.heron.healthmgr.sensors.BackPressureSensor;
import org.apache.heron.healthmgr.sensors.BufferSizeSensor;
import org.apache.heron.healthmgr.sensors.ExecuteCountSensor;

import static org.apache.heron.healthmgr.HealthPolicyConfigReader.PolicyConfigKey.HEALTH_POLICY_INTERVAL_MS;
import static org.apache.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisType.DIAGNOSIS_DATA_SKEW;
import static org.apache.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisType.DIAGNOSIS_SLOW_INSTANCE;
import static org.apache.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisType.DIAGNOSIS_UNDER_PROVISIONING;

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
                                  BackPressureSensor backPressureSensor,
                                  BufferSizeSensor bufferSizeSensor,
                                  ExecuteCountSensor executeCountSensor,
                                  BackPressureDetector backPressureDetector,
                                  LargeWaitQueueDetector largeWaitQueueDetector,
                                  ProcessingRateSkewDetector dataSkewDetector,
                                  WaitQueueSkewDetector waitQueueSkewDetector,
                                  UnderProvisioningDiagnoser underProvisioningDiagnoser,
                                  DataSkewDiagnoser dataSkewDiagnoser,
                                  SlowInstanceDiagnoser slowInstanceDiagnoser,
                                  ScaleUpResolver scaleUpResolver) {
    this.policyConfig = policyConfig;
    this.scaleUpResolver = scaleUpResolver;

    registerSensors(backPressureSensor, bufferSizeSensor, executeCountSensor);
    registerDetectors(backPressureDetector, largeWaitQueueDetector,
        waitQueueSkewDetector, dataSkewDetector);
    registerDiagnosers(underProvisioningDiagnoser, dataSkewDiagnoser, slowInstanceDiagnoser);
    registerResolvers(scaleUpResolver);

    setPolicyExecutionInterval(
        Duration.ofMillis((int) policyConfig.getConfig(HEALTH_POLICY_INTERVAL_MS.key(), 60000)));

    eventManager.addEventListener(TopologyUpdate.class, this);
  }

  @Override
  public Collection<Action> executeResolvers(Collection<Diagnosis> diagnosis) {
    DiagnosisTable diagnosisTable = DiagnosisTable.of(diagnosis);

    if (diagnosisTable.type(DIAGNOSIS_DATA_SKEW.text()).size() > 0) {
      LOG.warning("Data Skew diagnoses. This diagnosis does not have any resolver.");
    } else if (diagnosisTable.type(DIAGNOSIS_SLOW_INSTANCE.text()).size() > 0) {
      LOG.warning("Slow Instance diagnoses. This diagnosis does not have any resolver.");
    } else if (diagnosisTable.type(DIAGNOSIS_UNDER_PROVISIONING.text()).size() > 0) {
      return scaleUpResolver.resolve(diagnosis);
    }

    return Collections.EMPTY_LIST;
  }

  @Override
  public void onEvent(TopologyUpdate event) {
    int interval = (int) policyConfig.getConfig(CONF_WAIT_INTERVAL_MILLIS, 180000);
    LOG.info("Received topology update action event: " + event);
    setOneTimeDelay(Duration.ofMillis(interval));
  }
}
