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

package org.apache.heron.integration_test.core;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.heron.api.spout.IRichSpout;
import org.apache.heron.api.spout.ISpoutOutputCollector;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.TopologyContext;

/**
 * Class that emits tuples until a given condition is met. The rate of emission is throttled and
 * once the condition is met there is an additional fixed time period where new tuples are emitted.
 * Upon completion uploads the set of tuples emitted and acked to the state server.
 */
class EmitUntilConditionTestSpout extends IntegrationTestSpout {
  private static final long serialVersionUID = 5231279157676404046L;
  private static final Logger LOG = Logger.getLogger(EmitUntilConditionTestSpout.class.getName());
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final long postEmitSleepSeconds;
  private final long postConditionEmitSeconds;
  private final Condition untilCondition;
  private final String tuplesEmittedStateServerUrl;
  private int taskIndex;

  private List<String> tuplesEmitted;
  private boolean conditionCheckerRunning = false;
  private RuntimeException conditionCheckerException = null;
  private Long quittingTime = null;

  EmitUntilConditionTestSpout(IRichSpout delegateSpout,
                              Condition untilCondition,
                              String topologyStartedStateUrl,
                              String tuplesEmittedStateServerUrl) {
    super(delegateSpout, Integer.MAX_VALUE, topologyStartedStateUrl);
    this.untilCondition = untilCondition;
    this.tuplesEmittedStateServerUrl = tuplesEmittedStateServerUrl;
    this.postEmitSleepSeconds = 1;
    this.postConditionEmitSeconds = 10;
    this.tuplesEmitted = new ArrayList<>();
  }

  private void startConditionChecker() {
    Executors.newSingleThreadExecutor().execute(new Runnable() {

      @Override
      public void run() {
        try {
          untilCondition.satisfyCondition();
          quittingTime = System.currentTimeMillis() + postConditionEmitSeconds * 1000;
          // SUPPRESS CHECKSTYLE IllegalCatch
        } catch (RuntimeException e) {
          conditionCheckerException = e;
        }
      }
    });
  }

  @Override
  public void open(Map<String, Object> map,
                   TopologyContext topologyContext,
                   SpoutOutputCollector outputCollector) {
    super.open(map, topologyContext, new EmitReportingTestSpoutCollector(outputCollector));
    taskIndex = topologyContext.getThisTaskIndex();
  }

  @Override
  protected boolean doneEmitting() {
    return quittingTime != null && System.currentTimeMillis() >= quittingTime;
  }

  @Override
  public void nextTuple() {
    if (!conditionCheckerRunning) {
      startConditionChecker();
      conditionCheckerRunning = true;
    } else if (conditionCheckerException != null) {
      throw conditionCheckerException;
    }
    super.nextTuple();
  }

  @Override
  protected TimeUnit getPostEmitSleepTimeUnit() {
    return TimeUnit.SECONDS;
  }

  @Override
  protected long getPostEmitSleepTime() {
    return this.postEmitSleepSeconds;
  }

  @Override
  protected void handleAckedMessage(Object messageId, List<Object> tuple) {
    super.handleAckedMessage(messageId, tuple);
    try {
      tuplesEmitted.add(MAPPER.writeValueAsString(tuple.get(0)));
    } catch (JsonProcessingException e) {
      LOG.log(Level.SEVERE,
          "Could not convert map to JSONString: " + tuple.get(0).toString(), e);
    }
  }

  // Clear spout state during topology parallelism update. This is because during
  // topology parallelism update, if one instance (identified as container_id-task_id)
  // is not changed between the old and the new packing plans, it will not be killed, instead
  // it will be deactivated and then activated which results in instance state before the update
  // being reserved. On the contrary, if one instance is changed, instance state before the update
  // will be lost. The thing can happen to both spouts and bolts. But in this test, AT_LEAST_ONCE
  // only requires tuples emitted at spouts to be a subset of tuples processed at bolts, thus clear
  // spout state when update topology parallelism can guarantee test correctness no matter how
  // packing plan changes.
  @Override
  public void activate() {
    super.activate();
    tuplesEmitted.clear();
  }

  private final class EmitReportingTestSpoutCollector extends SpoutOutputCollector {

    private EmitReportingTestSpoutCollector(ISpoutOutputCollector delegate) {
      super(delegate);
    }

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
      List<Integer> result = super.emit(streamId, tuple, messageId);
      handleTuple(streamId, tuple);
      return result;
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
      super.emitDirect(taskId, streamId, tuple, messageId);
      handleTuple(streamId, tuple);
    }

    private void handleTuple(String streamId, List<Object> tuple) {
      if (Constants.INTEGRATION_TEST_CONTROL_STREAM_ID.equals(streamId)
          && tuple == TERMINAL_TUPLE) {
        // All tuples have been handled, push tuplesEmitted to state server
        sendTuplesEmittedToStateServer(tuplesEmitted);
      }
    }

    private void sendTuplesEmittedToStateServer(List<String> tuples) {
      String url = tuplesEmittedStateServerUrl + "_" + taskIndex;
      try {
        String tuplesSent = tuples.toString();
        LOG.info(String.format("Posting tuples emitted to %s: %s", url, tuplesSent));
        HttpUtils.httpJsonPost(url, tuplesSent);
      } catch (IOException | ParseException e) {
        throw new RuntimeException("Failure posting tuples emitted to " + url, e);
      }
    }
  }
}
