package com.twitter.heron.spi.utils;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Shutdown {
  private static final Logger LOG = Logger.getLogger(Shutdown.class.getName());

  private boolean terminated = false;
  private final Lock lock = new ReentrantLock();
  private final Condition terminateCondition = lock.newCondition();

  public void await() {
    try {
      lock.lock();
      while(!terminated)
        terminateCondition.await();
      lock.unlock();
    } catch (InterruptedException e) {
      LOG.info("Process received interruption, terminating...");
      lock.unlock();
    }
  }

  public void terminate() {
    lock.lock();
    terminated = true;
    terminateCondition.signal();
    lock.unlock();
  }
}
