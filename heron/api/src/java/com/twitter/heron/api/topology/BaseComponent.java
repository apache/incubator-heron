package com.twitter.heron.api.topology;

import java.util.Map;

public abstract class BaseComponent implements IComponent {
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }    
}
