package org.apache.storm.topology;

import org.apache.storm.topology.IComponent;
import org.apache.storm.spout.ISpout;

/**
 * When writing topologies using Java, {@link IRichBolt} and {@link IRichSpout} are the main interfaces
 * to use to implement components of the topology.
 *
 */
public interface IRichSpout extends ISpout, IComponent {

}
