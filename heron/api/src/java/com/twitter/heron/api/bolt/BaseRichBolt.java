package com.twitter.heron.api.bolt;

import com.twitter.heron.api.topology.BaseComponent;

public abstract class BaseRichBolt extends BaseComponent implements IRichBolt {
    @Override
    public void cleanup() {
    }    
}
