package com.twitter.heron.api.bolt;

import java.util.Map;
import com.twitter.heron.api.topology.BaseComponent;
import com.twitter.heron.api.topology.TopologyContext;

public abstract class BaseBasicBolt extends BaseComponent implements IBasicBolt {

    @Override
    public void prepare(Map heronConf, TopologyContext context) {
    }

    @Override
    public void cleanup() {
    }    
}
