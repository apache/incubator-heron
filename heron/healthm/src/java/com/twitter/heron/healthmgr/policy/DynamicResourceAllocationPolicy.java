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

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import com.microsoft.dhalion.api.IHealthPolicy;
import com.microsoft.dhalion.api.IResolver;
import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.diagnoser.Diagnosis;
import com.microsoft.dhalion.resolver.Action;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.HealthPolicyConfigReader;
import com.twitter.heron.healthmgr.common.HealthMgrConstants;
import com.twitter.heron.healthmgr.detectors.BackPressureDetector;
import com.twitter.heron.healthmgr.diagnosers.DataSkewDiagnoser;
import com.twitter.heron.healthmgr.diagnosers.SlowInstanceDiagnoser;
import com.twitter.heron.healthmgr.diagnosers.UnderProvisioningDiagnoser;

public class DynamicResourceAllocationPolicy implements IHealthPolicy {
  private HealthPolicyConfig policyConfig;
  private final BackPressureDetector backPressureDetector;

  private final UnderProvisioningDiagnoser underProvisioningDiagnoser;
  private final DataSkewDiagnoser dataSkewDiagnoser;
  private final SlowInstanceDiagnoser slowInstanceDiagnoser;

  @Inject
  DynamicResourceAllocationPolicy(HealthPolicyConfig policyConfig,
                                  BackPressureDetector backPressureDetector,
                                  UnderProvisioningDiagnoser underProvisioningDiagnoser,
                                  DataSkewDiagnoser dataSkewDiagnoser,
                                  SlowInstanceDiagnoser slowInstanceDiagnoser) {
    this.policyConfig = policyConfig;

    this.backPressureDetector = backPressureDetector;

    this.underProvisioningDiagnoser = underProvisioningDiagnoser;
    this.dataSkewDiagnoser = dataSkewDiagnoser;
    this.slowInstanceDiagnoser = slowInstanceDiagnoser;
  }

  @Override
  public List<Symptom> executeDetectors() {
    return backPressureDetector.detect();
  }

  @Override
  public List<Diagnosis> executeDiagnosers(List<Symptom> symptoms) {
    Diagnosis diagnoses = underProvisioningDiagnoser.diagnose(symptoms);
    List<Diagnosis> diagnosis = new ArrayList<>();
    diagnosis.add(diagnoses);
    return diagnosis;
  }

  @Override
  public long getInterval() {
    return Long.valueOf(policyConfig.getConfig(HealthMgrConstants.HEALTH_POLICY_INTERVAL));
  }

  @Override
  public List<Action> executeResolvers(IResolver resolver) {
    return null;
  }

  @Override
  public void close() {
  }
}
