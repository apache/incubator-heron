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
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Named;

import com.microsoft.dhalion.api.IResolver;
import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.diagnoser.Diagnosis;
import com.microsoft.dhalion.events.EventManager;
import com.microsoft.dhalion.resolver.Action;

import com.twitter.heron.healthmgr.common.HealthManagerEvents.ContainerRestart;
import com.twitter.heron.healthmgr.common.PhysicalPlanProvider;

import static com.twitter.heron.healthmgr.HealthManager.CONF_TOPOLOGY_NAME;
import static com.twitter.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisName.DIAGNOSIS_SLOW_INSTANCE;

public class RestartContainerResolver implements IResolver {
  private static final Logger LOG = Logger.getLogger(RestartContainerResolver.class.getName());

  private PhysicalPlanProvider physicalPlanProvider;
  private EventManager eventManager;
  private String topologyName;

  @Inject
  public RestartContainerResolver(@Named(CONF_TOPOLOGY_NAME) String topologyName,
      PhysicalPlanProvider physicalPlanProvider, EventManager eventManager) {
    this.topologyName = topologyName;
    this.physicalPlanProvider = physicalPlanProvider;
    this.eventManager = eventManager;
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

      try {
        // TODO: want to know which stmgr has backpressure
        String stmgrId = bpSymptom.getComponent().getName();
        String shellUrl = physicalPlanProvider.getShellUrl(stmgrId);
        if (shellUrl == null) {
          throw new MalformedURLException("stmgr not found " + stmgrId);
        }
        String urlStr = "http://" + shellUrl + "/killexecutor";
        URL url = new URL(urlStr);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("POST");

        con.setDoOutput(true);
        DataOutputStream out = new DataOutputStream(con.getOutputStream());
        out.writeBytes("secret=" + topologyName);
        out.flush();
        out.close();

        int status = con.getResponseCode();
        LOG.info("Restarting container: " + url.toString() + "; result: " + status);
        con.disconnect();

        ContainerRestart action = new ContainerRestart();
        LOG.info("Broadcasting container restart event");
        eventManager.onEvent(action);

        List<Action> actions = new ArrayList<>();
        actions.add(action);
        return actions;
      } catch (MalformedURLException e) {
        LOG.warning(e.getMessage());
      } catch (IOException e) {
        LOG.warning(e.getMessage());
      }
    }

    return null;
  }

  @Override
  public void close() {
  }
}
