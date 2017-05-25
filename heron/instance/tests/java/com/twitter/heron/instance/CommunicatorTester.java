//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.instance;

import java.io.IOException;

import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SlaveLooper;
import com.twitter.heron.common.basics.WakeableLooper;
import com.twitter.heron.proto.system.HeronTuples;
import com.twitter.heron.proto.system.Metrics;
import com.twitter.heron.resource.Constants;
import com.twitter.heron.resource.UnitTestHelper;

/**
 * Class to help write tests that require loopers and communicators
 */
public class CommunicatorTester {
  private final WakeableLooper testLooper;
  private final SlaveLooper slaveLooper;

  // Only one outStreamQueue, which is responsible for both control tuples and data tuples
  private final Communicator<HeronTuples.HeronTupleSet> outStreamQueue;

  // This blocking queue is used to buffer tuples read from socket and ready to be used by instance
  // For spout, it will buffer Control tuple, while for bolt, it will buffer data tuple.
  private final Communicator<HeronTuples.HeronTupleSet> inStreamQueue;
  private final Communicator<InstanceControlMsg> inControlQueue;
  private final Communicator<Metrics.MetricPublisherPublishMessage> slaveMetricsOut;

  public CommunicatorTester() throws IOException {
    this(new NIOLooper());
  }

  protected CommunicatorTester(WakeableLooper testLooper) {
    UnitTestHelper.addSystemConfigToSingleton();
    this.testLooper = testLooper;
    slaveLooper = new SlaveLooper();
    outStreamQueue = new Communicator<>(slaveLooper, testLooper);
    outStreamQueue.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);
    inStreamQueue = new Communicator<>(testLooper, slaveLooper);
    inStreamQueue.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);
    inControlQueue = new Communicator<>(testLooper, slaveLooper);

    slaveMetricsOut = new Communicator<>(slaveLooper, testLooper);
    slaveMetricsOut.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);
  }

  public void stop() throws NoSuchFieldException, IllegalAccessException {
    UnitTestHelper.clearSingletonRegistry();

    if (testLooper != null) {
      testLooper.exitLoop();
    }
    if (slaveLooper != null) {
      slaveLooper.exitLoop();
    }
  }

  public Communicator<Metrics.MetricPublisherPublishMessage> getSlaveMetricsOut() {
    return slaveMetricsOut;
  }

  public WakeableLooper getTestLooper() {
    return testLooper;
  }

  public SlaveLooper getSlaveLooper() {
    return slaveLooper;
  }

  public Communicator<InstanceControlMsg> getInControlQueue() {
    return inControlQueue;
  }

  public Communicator<HeronTuples.HeronTupleSet> getInStreamQueue() {
    return inStreamQueue;
  }

  public Communicator<HeronTuples.HeronTupleSet> getOutStreamQueue() {
    return outStreamQueue;
  }
}
