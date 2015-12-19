package backtype.storm.topology;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.spout.SpoutOutputCollectorImpl;

/**
 * When writing topologies using Java, {@link IRichBolt} and {@link IRichSpout} are the main interfaces
 * to use to implement components of the topology.
 *
 */
public class IRichSpoutDelegate implements com.twitter.heron.api.spout.IRichSpout {
  private IRichSpout delegate;
  private TopologyContext topologyContextImpl;
  private SpoutOutputCollectorImpl spoutOutputCollectorImpl;

  public IRichSpoutDelegate(IRichSpout delegate) {
    this.delegate = delegate;
  }

  @Override
  public void open(Map conf, com.twitter.heron.api.topology.TopologyContext context, 
                   com.twitter.heron.api.spout.SpoutOutputCollector collector) {
    topologyContextImpl = new TopologyContext(context);
    spoutOutputCollectorImpl = new SpoutOutputCollectorImpl(collector);
    delegate.open(conf, topologyContextImpl, spoutOutputCollectorImpl);
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public void activate() {
    delegate.activate();
  }

  @Override
  public void deactivate() {
    delegate.deactivate();
  }

  @Override
  public void nextTuple() {
    delegate.nextTuple();
  }

  @Override
  public void ack(Object msgId) {
    delegate.ack(msgId);
  }

  @Override
  public void fail(Object msgId) {
    delegate.fail(msgId);
  }

  @Override
  public void declareOutputFields(com.twitter.heron.api.topology.OutputFieldsDeclarer declarer) {
    OutputFieldsGetter getter = new OutputFieldsGetter(declarer);
    delegate.declareOutputFields(getter);
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return delegate.getComponentConfiguration();
  }
}
