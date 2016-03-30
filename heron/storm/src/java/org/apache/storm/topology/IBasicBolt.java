package org.apache.storm.topology;

import java.util.Map;

import org.apache.storm.topology.IComponent;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

public interface IBasicBolt extends IComponent {
    void prepare(Map stormConf, TopologyContext context);
    /**
     * Process the input tuple and optionally emit new tuples based on the input tuple.
     * 
     * All acking is managed for you. Throw a FailedException if you want to fail the tuple.
     */
    void execute(Tuple input, BasicOutputCollector collector);
    void cleanup();
}
