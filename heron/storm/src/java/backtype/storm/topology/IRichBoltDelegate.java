package backtype.storm.topology;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollectorImpl;
import backtype.storm.tuple.TupleImpl;
/**
 * When writing topologies using Java, {@link IRichBolt} and {@link IRichSpout} are the main interfaces
 * to use to implement components of the topology.
 *
 */
public class IRichBoltDelegate implements com.twitter.heron.api.bolt.IRichBolt {
  private IRichBolt delegate;
  private TopologyContext topologyContextImpl;
  private OutputCollectorImpl outputCollectorImpl;

  public IRichBoltDelegate(IRichBolt delegate) {
    this.delegate = delegate;
  }

  @Override
  public void prepare(Map conf, com.twitter.heron.api.topology.TopologyContext context, 
               com.twitter.heron.api.bolt.OutputCollector collector) {
    topologyContextImpl = new TopologyContext(context);
    outputCollectorImpl = new OutputCollectorImpl(collector);
    delegate.prepare(conf, topologyContextImpl, outputCollectorImpl);
  }

  @Override
  public void cleanup() {
    delegate.cleanup();
  }

  @Override
  public void execute(com.twitter.heron.api.tuple.Tuple tuple) {
    TupleImpl impl = new TupleImpl(tuple);
    delegate.execute(impl);
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
