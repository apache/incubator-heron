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

import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.dhalion.core.Action;
import com.microsoft.dhalion.core.Diagnosis;
import com.microsoft.dhalion.core.Measurement;
import com.microsoft.dhalion.core.Symptom;
import com.microsoft.dhalion.policy.HealthPolicyImpl;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.healthmgr.common.PhysicalPlanProvider;

/**
 * This Policy class
 * 1. has a toggle switch
 * 2. works with runtime config
 */
public class ToggleablePolicy extends HealthPolicyImpl {
  private static final Logger LOG =
      Logger.getLogger(ToggleablePolicy.class.getName());

  public static final String TOGGLE_CONFIG_KEY = "healthmgr.toggleablepolicy.running";
  public static final String TOGGLE_RUNTIME_CONFIG_KEY = TOGGLE_CONFIG_KEY + ":runtime";

  @Inject
  protected PhysicalPlanProvider physicalPlanProvider;
  protected boolean running = true;
  // `policyConfigKey` is the config item for this policy in the healthmgr.yaml
  protected String policyConfigKey;

  @Override
  public Collection<Measurement> executeSensors() {
    for (TopologyAPI.Config.KeyValue kv
        : physicalPlanProvider.get().getTopology().getTopologyConfig().getKvsList()) {
      LOG.info("kv " + kv.getKey() + " => " + kv.getValue());
      if (kv.getKey().equals(TOGGLE_RUNTIME_CONFIG_KEY)) {
        Boolean b = Boolean.parseBoolean(kv.getValue());
        if (Boolean.FALSE.equals(b) && running) {
          running = false;
          LOG.info("policy running status changed to False");
        } else if (Boolean.TRUE.equals(b) && !running) {
          running = true;
          LOG.info("policy running status changed to True");
        }
      }
    }

    if (running) {
      return super.executeSensors();
    } else {
      return new ArrayList<Measurement>();
    }
  }

  @Override
  public Collection<Symptom> executeDetectors(Collection<Measurement> measurements) {
    if (running) {
      return super.executeDetectors(measurements);
    } else {
      return new ArrayList<Symptom>();
    }
  }

  @Override
  public Collection<Diagnosis> executeDiagnosers(Collection<Symptom> symptoms) {
    if (running) {
      return super.executeDiagnosers(symptoms);
    } else {
      return new ArrayList<Diagnosis>();
    }
  }

  @Override
  public Collection<Action> executeResolvers(Collection<Diagnosis> diagnosis) {
    if (running) {
      return super.executeResolvers(diagnosis);
    } else {
      /*
       * TODO(dhalion):
       * If sub-class could access the `lastExecutionTimestamp`
       * and `oneTimeDelay`, avoid super method invocation.
       */
      return super.executeResolvers(new ArrayList<Diagnosis>());
    }
  }
}
