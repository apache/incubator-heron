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

package org.apache.heron.metricsmgr;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.heron.api.metric.CountMetric;
import org.apache.heron.api.metric.MultiCountMetric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.spy;

/**
 * Test MultiCountMetric impl that lets the consumer await() for a metric to reach a certain
 * value. Multiple expectedValues can be passed, in which case each call to await() will wait for
 * the next value to be reached.
 */
public final class LatchedMultiCountMetric extends MultiCountMetric {
  private final String expectedKey;
  private final Long[] expectedValues;
  private final CountDownLatch[] countDownLatches;
  private CountMetric spiedMetric;
  private int currentLatchIndex = 0;

  public LatchedMultiCountMetric(String expectedKey, Long... expectedValues) {
    this.expectedKey = expectedKey;
    this.expectedValues = expectedValues;
    this.countDownLatches = new CountDownLatch[expectedValues.length];
    for (int i = 0; i < expectedValues.length; i++) {
      this.countDownLatches[i] = new CountDownLatch(1);
    }
  }

  public void await(Duration timeout) {
    assertTrue(String.format(
        "Invalid attempt made to call await() %s times, with only %d expectedValues passed",
        currentLatchIndex + 1, countDownLatches.length),
        currentLatchIndex < countDownLatches.length);
    try {
      if (!countDownLatches[currentLatchIndex].await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
        if (spiedMetric != null) {
          fail(String.format(
              "After waiting for %s, the expected metric for key '%s' is %d, found %d",
              timeout, expectedKey, expectedValues[currentLatchIndex], spiedMetric.getValue()));

        } else {
          fail(String.format(
              "After waiting for %s, the expected metric for key '%s' is %d, "
                  + "but the value was never incremented",
              timeout, expectedKey, expectedValues[currentLatchIndex]));
        }
      }
    } catch (InterruptedException e) {
      fail(String.format(
          "Await latch interrupted before timeout of %s was reached: %s",
          timeout, e));
    }
    currentLatchIndex++;
  }

  @Override
  public CountMetric scope(final String key) {
    final CountMetric metric = super.scope(key);
    if (!expectedKey.equals(key)) {
      return metric;
    } else if (spiedMetric == null) {
      final CountMetric spy = spy(metric);

      Mockito.doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          metric.incr();
          countDown(metric);
          return null;
        }
      }).when(spy).incr();

      Mockito.doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          metric.incrBy((Long) invocation.getArguments()[0]);
          countDown(metric);
          return null;
        }
      }).when(spy).incrBy(anyLong());

      this.spiedMetric = spy;
    }

    return spiedMetric;
  }

  private void countDown(CountMetric metric) {
    if (metric.getValue().equals(expectedValues[currentLatchIndex])) {
      countDownLatches[currentLatchIndex].countDown();
    }
  }
}
