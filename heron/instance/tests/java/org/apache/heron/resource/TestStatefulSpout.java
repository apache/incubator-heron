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
package org.apache.heron.resource;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.common.basics.SingletonRegistry;

public class TestStatefulSpout extends BaseRichSpout implements IStatefulComponent<String, String> {
  @Override
  public void open(
      Map<String, Object> conf,
      TopologyContext context,
      SpoutOutputCollector collector) {
  }

  @Override
  public void nextTuple() {
    AtomicBoolean shouldStartEmit =
        (AtomicBoolean) SingletonRegistry.INSTANCE.getSingleton(Constants.SPOUT_SHOULD_START_EMIT);

    if (shouldStartEmit != null && !shouldStartEmit.get()) {
      return;
    }

    // actually "emit" the tuple
    CountDownLatch emitLatch =
        (CountDownLatch) SingletonRegistry.INSTANCE.getSingleton(Constants.EMIT_LATCH);

    if (emitLatch != null) {
      emitLatch.countDown();
    }
  }

  @Override
  public void initState(State<String, String> state) {
  }

  @Override
  public void preSave(String checkpointId) {
    CountDownLatch preSaveLatch =
        (CountDownLatch) SingletonRegistry.INSTANCE.getSingleton(Constants.PRESAVE_LATCH);

    if (preSaveLatch != null) {
      preSaveLatch.countDown();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
}
