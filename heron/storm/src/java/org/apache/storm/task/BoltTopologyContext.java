package org.apache.storm.task;

/**
 * A TopologyContext that is given to a bolt is actually an instance
 * of BoltTopologyContext. Currently spouts can store bolt specific
 * information in this structure.
 */
public class BoltTopologyContext extends TopologyContext {
  public BoltTopologyContext(com.twitter.heron.api.topology.TopologyContext delegate) {
    super(delegate);
  }
}
