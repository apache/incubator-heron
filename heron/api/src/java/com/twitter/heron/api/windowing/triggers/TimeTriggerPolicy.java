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
package com.twitter.heron.api.windowing.triggers;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.twitter.heron.api.exception.FailedException;
import com.twitter.heron.api.windowing.DefaultEvictionContext;
import com.twitter.heron.api.windowing.Event;
import com.twitter.heron.api.windowing.EvictionPolicy;
import com.twitter.heron.api.windowing.TriggerHandler;
import com.twitter.heron.api.windowing.TriggerPolicy;

/**
 * Invokes {@link TriggerHandler#onTrigger()} after the duration.
 */
public class TimeTriggerPolicy<T> implements TriggerPolicy<T> {
  private static final Logger LOG = Logger.getLogger(TimeTriggerPolicy.class.getName());

  private long duration;
  private final TriggerHandler handler;
  private final ScheduledExecutorService executor;
  private final EvictionPolicy<T> evictionPolicy;
  private ScheduledFuture<?> executorFuture;

  public TimeTriggerPolicy(long millis, TriggerHandler handler) {
    this(millis, handler, null);
  }

  public TimeTriggerPolicy(long millis, TriggerHandler handler, EvictionPolicy<T>
      evictionPolicy) {
    this.duration = millis;
    this.handler = handler;
    this.executor = Executors.newSingleThreadScheduledExecutor();
    this.evictionPolicy = evictionPolicy;
  }

  @Override
  public void track(Event<T> event) {
    checkFailures();
  }

  @Override
  public void reset() {
    checkFailures();
  }

  @Override
  public void start() {
    executorFuture = executor.scheduleAtFixedRate(newTriggerTask(), duration, duration, TimeUnit
        .MILLISECONDS);
  }

  @Override
  public void shutdown() {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException ie) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public String toString() {
    return "TimeTriggerPolicy{" + "duration=" + duration + '}';
  }

  /*
  * Check for uncaught exceptions during the execution
  * of the trigger and fail fast.
  * The uncaught exceptions will be wrapped in
  * ExecutionException and thrown when future.get() is invoked.
  */
  private void checkFailures() {
    if (executorFuture != null && executorFuture.isDone()) {
      try {
        executorFuture.get();
      } catch (InterruptedException ex) {
        LOG.severe(String.format("Got exception\n%s", ex));
        throw new FailedException(ex);
      } catch (ExecutionException ex) {
        LOG.severe(String.format("Got exception\n%s", ex));
        throw new FailedException(ex.getCause());
      }
    }
  }

  private Runnable newTriggerTask() {
    return new Runnable() {

      @Override
      @SuppressWarnings("IllegalCatch")
      public void run() {
        // do not process current timestamp since tuples might arrive while the trigger is executing
        long now = System.currentTimeMillis() - 1;
        try {
                    /*
                     * set the current timestamp as the reference time for the eviction policy
                     * to evict the events
                     */
          evictionPolicy.setContext(new DefaultEvictionContext(now, null, null, duration));
          handler.onTrigger();
        } catch (Throwable th) {
          LOG.severe(String.format("handler.onTrigger failed\n%s", th));
                    /*
                     * propagate it so that task gets canceled and the exception
                     * can be retrieved from executorFuture.get()
                     */
          throw th;
        }
      }
    };
  }
}
