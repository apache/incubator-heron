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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.dhalion.api.IHealthPolicy;
import com.microsoft.dhalion.api.IResolver;
import com.microsoft.dhalion.core.EventHandler;
import com.microsoft.dhalion.core.EventManager;
import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.diagnoser.Diagnosis;
import com.microsoft.dhalion.resolver.Action;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.common.HealthManagerEvents.TopologyUpdate;
import com.twitter.heron.healthmgr.common.HealthMgrConstants;
import com.twitter.heron.healthmgr.detectors.BackPressureDetector;
import com.twitter.heron.healthmgr.diagnosers.DataSkewDiagnoser;
import com.twitter.heron.healthmgr.diagnosers.SlowInstanceDiagnoser;
import com.twitter.heron.healthmgr.diagnosers.UnderProvisioningDiagnoser;
import com.twitter.heron.healthmgr.resolvers.ScaleUpResolver;

public class DynamicResourceAllocationPolicy
    implements IHealthPolicy, EventHandler<TopologyUpdate> {

  private static final Logger LOG
      = Logger.getLogger(DynamicResourceAllocationPolicy.class.getName());

  public static final String CONF_WAIT_INTERVAL_MILLIS =
      "DynamicResourceAllocationPolicy.conf_post_action_wait_interval_min";

  private HealthPolicyConfig policyConfig;
  private BackPressureDetector backPressureDetector;
  private UnderProvisioningDiagnoser underProvisioningDiagnoser;
  private DataSkewDiagnoser dataSkewDiagnoser;
  private SlowInstanceDiagnoser slowInstanceDiagnoser;
  private ScaleUpResolver scaleUpResolver;

  private ArrayList<Diagnosis> diagnosis;

  private long newTopologyEventTimeMills = 0;

  @Inject
  DynamicResourceAllocationPolicy(HealthPolicyConfig policyConfig,
                                  EventManager eventManager,
                                  BackPressureDetector backPressureDetector,
                                  UnderProvisioningDiagnoser underProvisioningDiagnoser,
                                  DataSkewDiagnoser dataSkewDiagnoser,
                                  SlowInstanceDiagnoser slowInstanceDiagnoser,
                                  ScaleUpResolver scaleUpResolver) {
    this.policyConfig = policyConfig;

    this.backPressureDetector = backPressureDetector;

    this.underProvisioningDiagnoser = underProvisioningDiagnoser;
    this.dataSkewDiagnoser = dataSkewDiagnoser;
    this.slowInstanceDiagnoser = slowInstanceDiagnoser;
    this.scaleUpResolver = scaleUpResolver;

    eventManager.addEventListener(TopologyUpdate.class, this);
    newTopologyEventTimeMills = System.currentTimeMillis();
  }

  @Override
  public List<Symptom> executeDetectors() {
    long delay = getDelay();
    if (delay > 0) {
      LOG.info("Skip detector execution to let topology stabilize, wait=" + delay);
      return new ArrayList<>();
    }
    return backPressureDetector.detect();
  }

  private long getDelay() {
    int interval = Integer.valueOf(policyConfig.getConfig(CONF_WAIT_INTERVAL_MILLIS, "180000"));
    return System.currentTimeMillis() - newTopologyEventTimeMills - interval;
  }


  @Override
  public List<Diagnosis> executeDiagnosers(List<Symptom> symptoms) {
    diagnosis = new ArrayList<>();

    Diagnosis diagnoses = underProvisioningDiagnoser.diagnose(symptoms);
    if (diagnoses != null) {
      diagnosis.add(diagnoses);
    }

    diagnoses = slowInstanceDiagnoser.diagnose(symptoms);
    if (diagnoses != null) {
      diagnosis.add(diagnoses);
    }

    diagnoses = dataSkewDiagnoser.diagnose(symptoms);
    if (diagnoses != null) {
      diagnosis.add(diagnoses);
    }

    return diagnosis;
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
  public long getInterval() {
    return Long.valueOf(policyConfig.getConfig(HealthMgrConstants.HEALTH_POLICY_INTERVAL));
  }

  @Override
  public List<Action> executeResolvers(IResolver resolver) {
    if (resolver == null) {
      return null;
    }

    List<Action> actions = resolver.resolve(diagnosis);
    if (actions != null && !actions.isEmpty()) {
    }

    return actions;
  }

  @Override
  public void onEvent(TopologyUpdate event) {
    LOG.info("Received topology update action event: " + event);
    newTopologyEventTimeMills = System.currentTimeMillis();
  }

  @Override
  public void close() {
  }
}
