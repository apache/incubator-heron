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

import java.util.Collection;
import java.util.concurrent.LinkedTransferQueue;

/**
 * An soft bounded unblocking queue based on LinkedTransferQueue.
 * This queue will has a soft bound, which mean you could check the remainingCapacity() to decide
 * whether you could continuously offer items. However, the buffer underneath is an unbounded
 * LinkedTransferQueue, so you could still offer items even if remainingCapacity() &lt;= 0.
 * <p>
 * We use an unbound queue since for every time user's bolt's executing or spout's emitting tuples,
 * it is possible that unbounded # of tuples could be generated. And we could not stop them since
 * the logic is inside their jars. So in order to avoid enqueue failure, we need an unbound queue.
 * <p>
 * However, in order to avoid GC issues and keep high performance, we would have a dynamical tuning
 * Queue's expected capacity, see updateExpectedAvailableCapacity() below.
 */

public class Communicator<E> {
  /**
   * The buffer queue underneath, an unbound queue.
   */
  private final LinkedTransferQueue<E> buffer;

  /*
   * The producer offers item into the queue, and it will be wake up when consumer polls a item.
   */
  private volatile WakeableLooper producer;

  /**
   * The consumer polls item from the queue, and it will be wake up when producer offer a item.
   */
  private volatile WakeableLooper consumer;

  /**
   * The soft capacity bound for this queue
   */
  private volatile int capacity;

  /**
   * Used in updateExpectedAvailableCapacity()
   * Variables related to dynamically tune Communicator's expected available capacity
   * <p>
   * The value should be positive number && smaller than the capacity
   * -- Non-positive number can cause starvation concerns.
   * -- Too large positive number can bring gc issues.
   */
  private volatile int expectedAvailableCapacity;

  /**
   * Used in updateExpectedAvailableCapacity()
   * Variables related to dynamically tune Communicator's expected available capacity
   */
  private volatile int expectedQueueSize;

  /**
   * The average size of LinkedTransferQueue<E> buffer.
   * We sample the size() in a interval so the size is an average value
   */
  private volatile int averageSize;

  /**
   * Used in updateExpectedAvailableCapacity()
   * Variables related to dynamically tune Communicator's expected available capacity
   */
  private volatile double currentSampleWeight;

  /**
   * Used to help control to size of queue manually.
   * By setting it true, getExpectedAvailableCapacity() would always return -1,
   * and external users could know they should not offer more items to the Communicator.
   * Notice: this is just an "expected" flag, and users could still invoke offer() to push more items
   * if they want.
   */
  private volatile boolean isExpectNoMoreItems;

  /**
   * Constructor for Communicator
   *
   * @param producer would be waken up when items are consumed from queue,
   * or set it to null if we don't want producer to be waken up
   * @param consumer would be waken up when items are produced into queue,
   * or set it to null if we don't want consumer to be waken up
   */
  public Communicator(WakeableLooper producer, WakeableLooper consumer) {
    this.producer = producer;
    this.consumer = consumer;
    this.buffer = new LinkedTransferQueue<E>();
  }

  public Communicator() {
    this.isExpectNoMoreItems = false;
    this.producer = null;
    this.consumer = null;
    this.buffer = new LinkedTransferQueue<E>();
  }

  public void setProducer(WakeableLooper producer) {
    this.producer = producer;
  }

  public void setConsumer(WakeableLooper consumer) {
    this.consumer = consumer;
  }

  public void init(int ipcapacity, int ipexpectedQueueSize, double ipcurrentSampleWeight) {
    this.capacity = ipcapacity;
    this.expectedQueueSize = ipexpectedQueueSize;
    this.currentSampleWeight = ipcurrentSampleWeight;

    // We set the default expected available capacity half as the capacity
    this.expectedAvailableCapacity = capacity / 2;

    // Notify both sides to pick up new values
    informConsumer();
    informProducer();
  }

  /**
   * Get the number of items in queue
   *
   * @return the number of items in queue
   */
  public int size() {
    return buffer.size();
  }

  public int remainingCapacity() {
    return capacity - size();
  }

  /**
   * Check if there is any item in the queue
   *
   * @return null if there is no item inside the queue
   */
  public E poll() {
    E result = buffer.poll();
    if (producer != null) {
      producer.wakeUp();
    }

    return result;
  }

  /**
   * Since it is an unbounded queue, the offer will always return true.
   *
   * @param e Item to be inserted
   * @return true : inserted successfully
   */
  public boolean offer(E e) {
    buffer.offer(e);
    if (consumer != null) {
      consumer.wakeUp();
    }

    return true;
  }

  public E peek() {
    return buffer.peek();
  }

  public int getCapacity() {
    return capacity;
  }

  public boolean isEmpty() {
    return buffer.isEmpty();
  }

  public void clear() {
    buffer.clear();
  }

  /**
   * Removes all available elements from this queue and adds them to the given collection.
   * This operation may be more efficient than repeatedly polling this queue.
   * A failure encountered while attempting to add elements to collection c may result in elements being in neither,
   * either or both collections when the associated exception is thrown. Attempts to drain a queue to itself result in IllegalArgumentException.
   * Further, the behavior of this operation is undefined if the specified collection is modified while the operation is in progress.
   *
   * @return the number of elements transferred
   */
  public int drainTo(Collection<? super E> c) {
    int result = buffer.drainTo(c);
    if (producer != null) {
      producer.wakeUp();
    }

    return result;
  }

  public int drainTo(Collection<? super E> c, int maxElements) {
    int result = buffer.drainTo(c, maxElements);
    if (producer != null) {
      producer.wakeUp();
    }

    return result;
  }

  public void updateExpectedAvailableCapacity() {
    // We use Exponential moving average: En = (1-w) * En-1 + w * An
    // http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average
    int inAvgSize
        = (int) ((1 - currentSampleWeight) * averageSize + currentSampleWeight * size());
    int availableCapacity = expectedAvailableCapacity;

    if (inAvgSize < expectedQueueSize
        && availableCapacity < capacity) {
      // The increase of available capacity is slow: just add one, since:
      // 1. We want the increase smoothly to hit the optimized value quickly when the value is
      // near the optimized value
      // 2. The default value is Constants.QUEUE_BUFFER_SIZE / 2, and in fact it will not take
      // long time to hit the optimized value
      expectedAvailableCapacity = availableCapacity + 1;
    }

    // Make sure expectedAvailableCapacity will still be positive number if we decrease it
    if (inAvgSize > expectedQueueSize && availableCapacity > 1) {
      // The decrease of available capacity is quick since
      // we want to recover quickly once we back-up items , which may cause GC issues
      expectedAvailableCapacity = availableCapacity / 2;
    }

    averageSize = inAvgSize;
  }

  public int getExpectedAvailableCapacity() {
    return isExpectNoMoreItems ? -1 : expectedAvailableCapacity;
  }

  public void expectNoMoreItems() {
    isExpectNoMoreItems = true;
    informProducer();
  }

  public void expectMoreItems() {
    isExpectNoMoreItems = false;
    informProducer();
  }

  public void informProducer() {
    if (producer != null) {
      producer.wakeUp();
    }
  }

  public void informConsumer() {
    if (consumer != null) {
      consumer.wakeUp();
    }
  }
}
