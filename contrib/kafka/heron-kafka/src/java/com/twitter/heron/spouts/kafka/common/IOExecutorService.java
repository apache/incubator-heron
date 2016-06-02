// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.spouts.kafka.common;

import java.util.ArrayList;
//CHECKSTYLE:OFF AvoidStarImport
import java.util.concurrent.*;
import java.util.logging.Logger;

public class IOExecutorService {
  /**
   * Wraps read function object which fetch data.
   *
   * @param <R> Return type of fn.
   * @param <T> Type of item read.
   * @param <X> Context object.
   */
  public interface FnParam<R, T, X> {
    /**
     * Function to be wrapped.
     *
     * @param stream  Stream object
     * @param context context
     * @return return for function.
     */
    R apply(BlockingQueue<T> stream, X context);
  }

  /**
   * Executes an IO Task in a separate thread. It guarantees following -
   * 1. Only one task is active running
   * 2. No task is scheduled. Task scheduled when another task is active will fail immediately.
   * 3. Task is passed a Stream, where it can lazily output records fetched so far.
   * <p/>
   * Task running on IOExecutorService can block in read, without blocking Main thread or consumer
   * which is draining queue. Also, performance will not degrade if Main thread doesn't have
   * an ideal algorithm to schedule IO Task correctly, since it can call schedule at any rate.
   * Task can spawn multiple threads internally and stream response from them.
   * IOExecutorService limits the size of task queue and simplifies the task of ensuring block free
   * Main thread.
   * SampleUse:
   * <code>
   * val fn = new FnParam<Boolean, FooThrift, Option[Context]>() {
   * override def fn(s:BufferStream[FooThrift], ctx: Option[Context]): Boolean = {
   * fetchItem(ctx, ...) foreach { (item) => s offer item }
   * }
   * val stream = new BufferedStream[FooThrift] (1 * K)
   * val ex = new SingleThreadIOExecutorService[FooThrift, Context](fn, None, stream)
   * ex.scheduleFixedInterval(10)
   * ex.scheduleContinuously()
   * while(true) {
   * val item = stream.poll(100, TimeUnit.MILLISECONDS)
   * if (item!= null) process(item)
   * otherStuffWhichShouldNotBlockButHaveToRunHere()
   * }
   * </code>
   *
   * @param <R> Return type for read function.
   * @param <T> Type of object produced.
   * @param <X> Context object passed to read function. Between subsequent calls to read, same (or
   *            a copy of) context object is passed. No attempts is made to keep context same
   *            across restart. TODO: make it possible.
   */
  public static class SingleThreadIOExecutorService<R, T, X> {
    private static final Logger LOG =
        Logger.getLogger(SingleThreadIOExecutorService.class.getName());

    private final FnParam<R, T, X> readFn;
    private final X ctx;
    private final BlockingQueue<T> stream;
    private final ExecutorService threadPool;

    private ArrayList<ScheduledExecutorService> timers = new ArrayList<>();
    private volatile boolean runContinuously = false;
    private volatile Object lock = new Object();
    private volatile boolean isActive = false;
    private final Runnable runnable = new Runnable() {
      public void run() {
        readFn.apply(stream, ctx);
        isActive = false;
        if (runContinuously) {
          execute();
        }
      }
    };

    /**
     * Creates a Single threaded IO Task executor.
     *
     * @param readFn Task Function which takes a stream and a Context and read items from an IO
     *               source. This function generate output only on LazyStream and shouldn't
     *               access/modify object outside X object passed to it.
     * @param ctx    Shared context object between subsequent read calls. Note: Only one thread is
     *               active so, context object needn't be thread-safe.
     * @param stream Output stream.
     */

    public SingleThreadIOExecutorService(FnParam<R, T, X> readFn, X ctx, BlockingQueue<T> stream) {
      this(readFn, ctx, stream, Executors.newSingleThreadExecutor());
    }

    protected SingleThreadIOExecutorService(
        FnParam<R, T, X> readFn, X ctx, BlockingQueue<T> stream, ExecutorService threadpool) {
      this.readFn = readFn;
      this.ctx = ctx;
      this.stream = stream;
      this.threadPool = threadpool;
    }

    /**
     * Starts running read function subject to above guarantee.
     *
     * @return True if this call successfully scheduled starts read, false otherwise.
     */
    public boolean execute() {
      synchronized (lock) {
        if (isActive) {
          return false;
        }
        isActive = true;
      }
      threadPool.execute(runnable);
      return true;
    }

    /**
     * Turns off all executor and shutdown the threadpool.
     *
     * @param timeout Time to wait to allow graceful shutdown
     * @param unit    Timeunit.
     */
    public void terminate(long timeout, TimeUnit unit) {
      // Cancel all timers
      for (ScheduledExecutorService t : timers) {
        t.shutdownNow();
      }
      timers = new ArrayList<>();
      // Send termination to threadpool
      threadPool.shutdownNow();
      try {
        threadPool.awaitTermination(timeout, unit);
      } catch (InterruptedException ie) {
        LOG.severe("Threadpool shutdown interrupted");
      }
    }

    /**
     * Schedule immediately when a read completes
     */
    public void scheduleContinuously() {
      runContinuously = true;
      execute();
    }

    /**
     * Schedule execute recurrent. This will call execute method at fixed interval.
     *
     * @param millis Time in millis, to attempt to schedule IO Task. 0 implies busy attempt.
     */
    public void scheduleFixedInterval(long millis) {
      scheduleWhen("scheduleFixed", millis, new FnParam<Boolean, T, X>() {
        public Boolean apply(BlockingQueue<T> s, X x) {
          return true;
        }
      });
    }

    /**
     * Polls periodically if LazyStream is empty. Triggers IO Task if it is true.
     *
     * @param millis Time in millis, to attempt to schedule IO Task. 0 implies spin poll.
     */
    public void scheduleWhenEmpty(long millis) {
      scheduleWhen("scheduleEmpty", millis, new FnParam<Boolean, T, X>() {
        public Boolean apply(BlockingQueue<T> s, X x) {
          return s.isEmpty();
        }
      });
    }

    /**
     * Attempts to schedule when a condition is satisfied.
     *
     * @param millis    Polling duration to verify condition has been met
     * @param condition Function which returns true, if new IO Task scheduling attempt can be made.
     */
    public void scheduleWhen(
        final String name, long millis, final FnParam<Boolean, T, X> condition) {
      ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
          new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
              Thread t = new Thread(r, name);
              t.setDaemon(false);
              return t;
            }
          });

      timers.add(timer);

      timer.scheduleAtFixedRate(new Runnable() {
        public void run() {
          if (condition.apply(stream, ctx)) {
            execute();
          }
        }
      }, 0, millis, TimeUnit.MILLISECONDS);
    }
  }
}
