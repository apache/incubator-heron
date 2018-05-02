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

package org.apache.heron.instance;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.heron.common.basics.SlaveLooper;

/**
 * Class to help write tests that require Slave instances, loopers and communicators
 */
public class SlaveTester extends CommunicatorTester {
  private final ExecutorService threadsPool;
  private final Slave slave;

  public SlaveTester() {
    this(null);
  }

  public SlaveTester(CountDownLatch outStreamQueueOfferLatch) {
    super(new SlaveLooper(), outStreamQueueOfferLatch);
    slave = new Slave(getSlaveLooper(), getInStreamQueue(), getOutStreamQueue(),
        getInControlQueue(), getSlaveMetricsOut());
    threadsPool = Executors.newSingleThreadExecutor();
  }

  public void start() {
    threadsPool.execute(slave);
  }

  public void stop() throws NoSuchFieldException, IllegalAccessException {
    super.stop();

    if (threadsPool != null) {
      threadsPool.shutdownNow();
    }
  }
}
