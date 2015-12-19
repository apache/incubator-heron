package backtype.storm.topology.base;

import backtype.storm.topology.base.BaseComponent;
import backtype.storm.topology.IRichSpout;

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
