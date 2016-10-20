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
package com.twitter.heron.integration_test.core;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.logging.Logger;

import com.twitter.heron.api.spout.IRichSpout;

/**
 * Spout that delegates to another spout to emit tuples, satisfy a restartCondition and repeat, up
 * to totalPhases times.
 */
class MultiPhaseTestSpout extends IntegrationTestSpout {
  private static final Logger LOG = Logger.getLogger(MultiPhaseTestSpout.class.getName());
  private static final long serialVersionUID = 4375157636632941400L;

  private final Condition restartCondition;
  private final String httpStateServerUrl;
  private final int executionsPerPhase;
  private final int totalPhases;
  private int phasesComplete;
  private boolean hasSetStarted;

  interface Condition extends Serializable {
    void satisfyCondition();
  }

  MultiPhaseTestSpout(IRichSpout delegateSpout, int executionsPerPhase,
                      int totalPhases, Condition restartCondition, String httpStateServerUrl) {
    super(delegateSpout, executionsPerPhase);
    this.executionsPerPhase = executionsPerPhase;
    this.totalPhases = totalPhases;
    this.restartCondition = restartCondition;
    this.httpStateServerUrl = httpStateServerUrl;
    this.phasesComplete = 0;
    this.hasSetStarted = false;
  }

  @Override
  public void nextTuple() {
    if (!this.hasSetStarted) {
      setStateToStarted();
      this.hasSetStarted = true;
    }
    super.nextTuple();
  }

  @Override
  protected void emitTerminalIfNeeded() {
    if (doneEmitting() && doneAcking()) {
      phasesComplete++;
      LOG.info(String.format("Completed phase %d of %d", phasesComplete, totalPhases));
      if (phasesComplete == totalPhases) {
        super.emitTerminalIfNeeded();
      } else {
        LOG.info(String.format("Satisfying restartCondition before starting phase %d",
            phasesComplete + 1));
        restartCondition.satisfyCondition();
        LOG.info(String.format("Resetting maxExecutions=%s to start phase %d",
            executionsPerPhase, phasesComplete + 1));
        super.resetMaxExecutions(this.executionsPerPhase);
      }
    }
  }

  private void setStateToStarted() {
    String url = httpStateServerUrl + "_topology_started";
    try {
      HttpUtils.httpJsonPost(url, "\"true\"");
    } catch (IOException | ParseException e) {
      throw new RuntimeException("Failure posting topology_started state to " + url, e);
    }
  }
}
