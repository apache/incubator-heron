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
package com.twitter.heron.common.basics;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test communicator that provides the ability to await a certain number of expected offers to be
 * received before proceeding by calling the awaitOffers method.
 * @param <E>
 */
public class TestCommunicator<E> extends Communicator<E> {
  private final CountDownLatch offersReceivedLatch;

  public TestCommunicator(int numExpectedOffers) {
    super();
    this.offersReceivedLatch = new CountDownLatch(numExpectedOffers);
  }

  /**
   * Returns one the number of offers received has reached numExpectedOffers.
   * @param timeout how long to await the offers to reach numExpectedOffers
   */
  public void awaitOffers(Duration timeout) throws InterruptedException {
    offersReceivedLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean offer(E e) {
    boolean response = super.offer(e);
    offersReceivedLatch.countDown();
    return response;
  }
}
