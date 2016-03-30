package org.apache.storm.task;

/**
 * A TopologyContext that is given to a spout is actually an instance
 * of SpoutTopologyContext. Currently spouts can store spout specific
 * information in this structure.
 */
public class SpoutTopologyContext extends TopologyContext {
  public SpoutTopologyContext(com.twitter.heron.api.topology.TopologyContext delegate) {
    super(delegate);
  }

  /**
   * Gets the Maximum Spout Pending value for this instance of spout.
   */
  public Long getMaxSpoutPending() {
    throw new RuntimeException("Heron does not support Auto MSP");
  }

  /**
   * Sets the Maximum Spout Pending value for this instance of spout
   */
  public void setMaxSpoutPending(Long maxSpoutPending) {
    throw new RuntimeException("Heron does not support Auto MSP");
  }
}
