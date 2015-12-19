package com.twitter.heron.api.spout;

import com.twitter.heron.api.topology.IComponent;

/**
 * When writing topologies using Java, {@link IRichBolt} and {@link IRichSpout} are the main interfaces
 * to use to implement components of the topology.
 *
 */
public interface IRichSpout extends ISpout, IComponent {

}
