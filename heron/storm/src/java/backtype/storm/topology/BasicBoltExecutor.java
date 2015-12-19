package backtype.storm.topology;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.ReportedFailedException;
import backtype.storm.task.OutputCollector;

public class BasicBoltExecutor implements IRichBolt {
    private IBasicBolt delegate;
    private transient BasicOutputCollector collector;
    
    public BasicBoltExecutor(IBasicBolt bolt) {
        this.delegate = bolt;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        delegate.declareOutputFields(declarer);
    }

    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        delegate.prepare(stormConf, context);
        this.collector = new BasicOutputCollector(collector);
    }

    @Override
    public void execute(Tuple input) {
        this.collector.setContext(input);
        try {
            delegate.execute(input, collector);
            this.collector.getOutputter().ack(input);
        } catch(FailedException e) {
            if(e instanceof ReportedFailedException) {
                this.collector.reportError(e);
            }
            this.collector.getOutputter().fail(input);
        }
    }

    @Override
    public void cleanup() {
        delegate.cleanup();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return delegate.getComponentConfiguration();
    }
}
