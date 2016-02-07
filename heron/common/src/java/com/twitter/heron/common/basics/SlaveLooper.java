package com.twitter.heron.common.basics;

/**
 * A SlaveLooper, implementing WakeableLooper, is a class wrapping object wait()/notify() to await/unblock a thread.
 * It extends WakeableLooper, so it will execute in a while loop unless the exitLoop() is called.
 * And in every execution, in tasksOnWakeup(), it will do nothing by default
 * The SlaveLooper should start by calling {@code loop()}
 */

public class SlaveLooper extends WakeableLooper {
  // The lock to implement the await/unblock
  private final RunnableLock lock;

  public SlaveLooper() {
    this.lock = new RunnableLock();
  }

  @Override
  protected void doWait() {
    synchronized (lock.proceedLock) {
      while (!lock.isToProceed) {

        // If timer task exists, the doWait() should wait not later than the time timer to execute
        // It no timer exists, we consider it will wait forever until other threads call wakeUp()
        // The nextTimeoutIntervalMs is in milli-seconds
        long nextTimeoutIntervalMs = getNextTimeoutIntervalMs();

        // In fact, only when the timeout > 0 (no timer should be executed before now)
        // or no wakeUp() is called during the thread's run, will the thread wait().
        if (nextTimeoutIntervalMs > 0) {
          try {
            // The wait will take the timeout in unit of milli-seconds
            lock.proceedLock.wait(nextTimeoutIntervalMs);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        } else {
          // break the loop if timeout happens
          break;
        }
      }
      lock.isToProceed = false;
    }
  }

  @Override
  public void wakeUp() {
    // In fact, we are using the wait()/notify() to implement the blocking thread here
    if (!lock.isToProceed) {
      synchronized (lock.proceedLock) {
        lock.isToProceed = true;
        lock.proceedLock.notify();
      }
    }
  }

  //The lock used to await/unblock the thread
  private static final class RunnableLock {
    private Object proceedLock;
    private volatile boolean isToProceed;

    public RunnableLock() {
      this.proceedLock = new Object();
      isToProceed = false;
    }
  }
}
