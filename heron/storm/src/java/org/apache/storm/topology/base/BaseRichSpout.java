package org.apache.storm.topology.base;

import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.base.BaseComponent;

public abstract class BaseRichSpout extends BaseComponent implements IRichSpout {
    @Override
    public void close() {
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }
}
