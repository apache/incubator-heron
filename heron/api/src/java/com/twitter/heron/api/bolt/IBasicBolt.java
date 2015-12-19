package com.twitter.heron.api.bolt;

import com.twitter.heron.api.topology.IComponent;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;
import java.util.Map;

public interface IBasicBolt extends IComponent {
    void prepare(Map heronConf, TopologyContext context);
    /**
     * Process the input tuple and optionally emit new tuples based on the input tuple.
     * 
     * All acking is managed for you. Throw a FailedException if you want to fail the tuple.
     */
    void execute(Tuple input, BasicOutputCollector collector);
    void cleanup();
}
