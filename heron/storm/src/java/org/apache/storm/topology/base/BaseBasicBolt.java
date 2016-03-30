package org.apache.storm.topology.base;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.base.BaseComponent;

public abstract class BaseBasicBolt extends BaseComponent implements IBasicBolt {

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void cleanup() {
    }    
}
