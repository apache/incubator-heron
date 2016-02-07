package com.twitter.heron.instance;

import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.proto.system.HeronTuples;

/**
 * Implementing this interface allows an object to be target of HeronInstance
 */

public interface IInstance {
  /**
   * Do the basic setup for HeronInstance
   */
  public void start();

  /**
   * Do the basic clean for HeronInstance
   * Notice: this method is not guaranteed to invoke
   * TODO: - to avoid confusing, we in fact have never called this method yet
   * TODO: - need to consider whether or not call this method more carefully
   */
  public void stop();

  /**
   * Read tuples from a queue and process the tuples
   *
   * @param inQueue the queue to read tuples from
   */
  public void readTuplesAndExecute(Communicator<HeronTuples.HeronTupleSet> inQueue);

  /**
   * Activate the instance
   */
  public void activate();

  /**
   * Deactivate the instance
   */
  public void deactivate();
}
