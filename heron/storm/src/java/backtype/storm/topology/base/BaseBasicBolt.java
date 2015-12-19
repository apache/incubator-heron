package backtype.storm.topology.base;

import java.util.Map;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.base.BaseComponent;
import backtype.storm.task.TopologyContext;

public abstract class BaseBasicBolt extends BaseComponent implements IBasicBolt {

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void cleanup() {
    }    
}
