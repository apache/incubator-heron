package org.apache.storm.topology.base;

import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.base.BaseComponent;

public abstract class BaseRichBolt extends BaseComponent implements IRichBolt {
    @Override
    public void cleanup() {
    }    
}
