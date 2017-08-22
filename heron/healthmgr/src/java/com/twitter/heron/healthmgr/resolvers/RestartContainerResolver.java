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
package com.twitter.heron.healthmgr.resolvers;

import java.io.DataOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.microsoft.dhalion.api.IResolver;
import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.diagnoser.Diagnosis;
import com.microsoft.dhalion.events.EventManager;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;
import com.microsoft.dhalion.resolver.Action;

import com.twitter.heron.api.generated.TopologyAPI.Topology;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.healthmgr.common.HealthManagerEvents.TopologyUpdate;
import com.twitter.heron.healthmgr.common.PackingPlanProvider;
import com.twitter.heron.healthmgr.common.TopologyProvider;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.system.PackingPlans;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.IRepacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlanProtoSerializer;
import com.twitter.heron.spi.utils.ReflectionUtils;

import static com.twitter.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisName.DIAGNOSIS_SLOW_INSTANCE;

public class RestartContainerResolver implements IResolver {
  private static final Logger LOG = Logger.getLogger(RestartContainerResolver.class.getName());

  private TopologyProvider topologyProvider;
  private PackingPlanProvider packingPlanProvider;
  private ISchedulerClient schedulerClient;
  private EventManager eventManager;
  private Config config;

  @Inject
  public RestartContainerResolver(TopologyProvider topologyProvider,
      PackingPlanProvider packingPlanProvider, ISchedulerClient schedulerClient,
      EventManager eventManager, Config config) {
    this.topologyProvider = topologyProvider;
    this.packingPlanProvider = packingPlanProvider;
    this.schedulerClient = schedulerClient;
    this.eventManager = eventManager;
    this.config = config;
  }

  @Override
  public List<Action> resolve(List<Diagnosis> diagnosis) {
    for (Diagnosis diagnoses : diagnosis) {
      Symptom bpSymptom = diagnoses.getSymptoms().get(DIAGNOSIS_SLOW_INSTANCE.text());
      if (bpSymptom == null || bpSymptom.getComponents().isEmpty()) {
        // nothing to fix as there is no back pressure
        continue;
      }

      if (bpSymptom.getComponents().size() > 1) {
        throw new UnsupportedOperationException("Multiple components with back pressure symptom");
      }

      // TODO: get the backpressure stmgr id from Diagnosis, then get ip:port from physical plan
      URL url = new URL("http://10.0.0.10:1234/killexecutor");
      HttpURLConnection con = (HttpURLConnection) url.openConnection();
      con.setRequestMethod("POST");

      con.setDoOutput(true);
      DataOutputStream out = new DataOutputStream(con.getOutputStream());
      out.writeBytes("secret=" + topologyProvider.get().getId());
      out.flush();
      out.close();

      int status = con.getResponseCode();
      LOG.info("Restarting container: " + url.toString() + "; result: " + status);
      con.disconnect();

      TopologyUpdate action = new TopologyUpdate();
      LOG.info("Broadcasting topology update event");
      eventManager.onEvent(action);

      List<Action> actions = new ArrayList<>();
      actions.add(action);
      return actions;
    }

    return null;
  }

  @Override
  public void close() {}
}
