package org.apache.storm.topology;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.ReportedFailedException;
import org.apache.storm.tuple.Tuple;

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
