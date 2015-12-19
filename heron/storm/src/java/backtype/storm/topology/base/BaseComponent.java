package backtype.storm.topology.base;

import java.util.Map;

import backtype.storm.topology.IComponent;

public abstract class BaseComponent implements IComponent {
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }    
}
