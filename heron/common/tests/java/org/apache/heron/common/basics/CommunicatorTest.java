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

package org.apache.heron.common.basics;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Communicator Tester.
 */
public class CommunicatorTest {
  private static final int QUEUE_BUFFER_SIZE = 128;
  private Communicator<Integer> communicator;
  private WakeableLooper producer;
  private WakeableLooper consumer;

  @Before
  public void before() {
    producer = new ExecutorLooper();
    consumer = new ExecutorLooper();
    communicator = new Communicator<Integer>(producer, consumer);
    communicator.init(QUEUE_BUFFER_SIZE, QUEUE_BUFFER_SIZE, 0.5);
  }

  @After
  public void after() {
    communicator = null;
    producer = null;
    consumer = null;
  }

  /**
   * Method: size()
   */
  @Test
  public void testSize() {
    for (int i = 0; i < QUEUE_BUFFER_SIZE; ++i) {
      communicator.offer(i);
      Assert.assertEquals(i + 1, communicator.size());
    }

    for (int i = QUEUE_BUFFER_SIZE; i > 0; --i) {
      communicator.poll();
      Assert.assertEquals(i - 1, communicator.size());
    }
  }

  /**
   * Method: remainingCapacity()
   */
  @Test
  public void testRemainingCapacity() {
    for (int i = QUEUE_BUFFER_SIZE; i > 0; --i) {
      communicator.offer(i);
      Assert.assertEquals(i - 1, communicator.remainingCapacity());
    }

    for (int i = 0; i < QUEUE_BUFFER_SIZE; ++i) {
      communicator.poll();
      Assert.assertEquals(i + 1, communicator.remainingCapacity());
    }
  }

  /**
   * Method: poll()
   */
  @Test
  public void testPoll() {
    for (int i = 0; i < QUEUE_BUFFER_SIZE; i++) {
      communicator.offer(i);
    }

    for (int i = 0; i < QUEUE_BUFFER_SIZE; i++) {
      Assert.assertEquals(i, communicator.poll().intValue());
    }

    Assert.assertNull(communicator.poll());
  }

  /**
   * Method: offer(E e)
   */
  @Test
  public void testOffer() {
    for (int i = 0; i < QUEUE_BUFFER_SIZE; i++) {
      communicator.offer(i);
    }

    for (int i = 0; i < QUEUE_BUFFER_SIZE; i++) {
      Assert.assertEquals(i, communicator.poll().intValue());
    }
  }

  @Test
  public void testOverOffer() {
    // The queue is soft bounded, hence over offer is allowed.
    for (int i = 0; i < QUEUE_BUFFER_SIZE + 2; i++) {
      communicator.offer(i);
    }

    for (int i = 0; i < QUEUE_BUFFER_SIZE + 2; i++) {
      Assert.assertEquals(i, communicator.poll().intValue());
    }
  }

  /**
   * Method: peek()
   */
  @Test
  public void testPeek() {
    for (int i = 0; i < QUEUE_BUFFER_SIZE; i++) {
      communicator.offer(i);
    }
    for (int i = 0; i < QUEUE_BUFFER_SIZE; i++) {
      Assert.assertEquals(i, communicator.peek().intValue());
      communicator.poll();
    }
  }

  /**
   * Method: isEmpty()
   */
  @Test
  public void testIsEmpty() {
    Assert.assertTrue(communicator.isEmpty());
    communicator.offer(1);
    Assert.assertFalse(communicator.isEmpty());
    communicator.poll();
    Assert.assertTrue(communicator.isEmpty());
  }

  /**
   * Method: getCapacity()
   */
  @Test
  public void testGetCapacity() {
    Assert.assertEquals(QUEUE_BUFFER_SIZE, communicator.getCapacity());
    communicator.offer(1);
    Assert.assertEquals(QUEUE_BUFFER_SIZE, communicator.getCapacity());
  }
}

