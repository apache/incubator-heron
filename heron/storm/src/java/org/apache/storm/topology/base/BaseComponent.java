package org.apache.storm.topology.base;

import java.util.Map;

import org.apache.storm.topology.IComponent;

public abstract class BaseComponent implements IComponent {
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }    
}
