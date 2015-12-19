package com.twitter.heron.common.core.base;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Communicator Tester.
 */
public class CommunicatorTest {
  Communicator<Integer> communicator;
  WakeableLooper producer;
  WakeableLooper consumer;
  final int QUEUE_BUFFER_SIZE = 128;

  @Before
  public void before() throws Exception {
    producer = new SlaveLooper();
    consumer = new SlaveLooper();
    communicator = new Communicator<Integer>(producer, consumer);
    communicator.init(QUEUE_BUFFER_SIZE, QUEUE_BUFFER_SIZE, 0.5);
  }

  @After
  public void after() throws Exception {
    communicator = null;
    producer = null;
    consumer = null;
  }

  /**
   * Method: size()
   */
  @Test
  public void testSize() throws Exception {
    for (int i = 0; i < 1024 * 1024; i++) {
      if (i % QUEUE_BUFFER_SIZE == 0) {
        communicator = new Communicator<Integer>(producer, consumer);
        communicator.init(QUEUE_BUFFER_SIZE, QUEUE_BUFFER_SIZE, 0.5);
      }
      communicator.offer(i);
      Assert.assertEquals((i % QUEUE_BUFFER_SIZE) + 1, communicator.size());
    }
  }

  /**
   * Method: remainingCapacity()
   */
  @Test
  public void testRemainingCapacity() throws Exception {
    for (int i = 0; i < 1024 * 1024; i++) {
      if (i % QUEUE_BUFFER_SIZE == 0) {
        communicator = new Communicator<Integer>(producer, consumer);
        communicator.init(QUEUE_BUFFER_SIZE, QUEUE_BUFFER_SIZE, 0.5);
      }
      communicator.offer(i);
      Assert.assertEquals(QUEUE_BUFFER_SIZE - (i % QUEUE_BUFFER_SIZE) - 1,
          communicator.remainingCapacity());
    }
  }

  /**
   * Method: poll()
   */
  @Test
  public void testPoll() throws Exception {
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
  public void testOffer() throws Exception {
    for (int i = 0; i < QUEUE_BUFFER_SIZE; i++) {
      communicator.offer(i);
    }

    for (int i = 0; i < QUEUE_BUFFER_SIZE; i++) {
      Assert.assertEquals(i, communicator.poll().intValue());
    }

  }

  /**
   * Method: peek()
   */
  @Test
  public void testPeek() throws Exception {
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
  public void testIsEmpty() throws Exception {
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
  public void testGetCapacity() throws Exception {
    Assert.assertEquals(QUEUE_BUFFER_SIZE, communicator.getCapacity());
  }
} 
