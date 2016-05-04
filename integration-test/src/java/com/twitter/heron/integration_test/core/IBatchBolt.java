package com.twitter.heron.integration_test.core;

import java.io.Serializable;

import com.twitter.heron.api.topology.IComponent;

// We keep this since we want to be consistent with earlier framework to reuse test topologies
public interface IBatchBolt extends Serializable, IComponent {
  /**
   * Invoke finishBatch() when the bolt is done, e.g. receives corresponding terminal signals
   */
  void finishBatch();
}
