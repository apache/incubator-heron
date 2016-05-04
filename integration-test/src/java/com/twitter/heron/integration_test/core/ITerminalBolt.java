package com.twitter.heron.integration_test.core;

import java.io.Serializable;

// We keep this since we want to be consistent with earlier framework to reuse test topologies
public interface ITerminalBolt extends Serializable {
  /**
   * Invoke to write all aggregated data to the destination
   * Destination can be http URL, local file, hdfs, etc.
   */
  void writeFinishedData();
}
