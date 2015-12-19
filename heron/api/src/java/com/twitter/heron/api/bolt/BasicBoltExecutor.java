package com.twitter.heron.api.bolt;

import java.util.Map;

import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.exception.FailedException;
import com.twitter.heron.api.exception.ReportedFailedException;

public class BasicBoltExecutor implements IRichBolt {
    private IBasicBolt _bolt;
    private transient BasicOutputCollector _collector;
    
    public BasicBoltExecutor(IBasicBolt bolt) {
        _bolt = bolt;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        _bolt.declareOutputFields(declarer);
    }

    
    @Override
    public void prepare(Map heronConf, TopologyContext context, OutputCollector collector) {
        _bolt.prepare(heronConf, context);
        _collector = new BasicOutputCollector(collector);
    }

    @Override
    public void execute(Tuple input) {
        _collector.setContext(input);
        try {
            _bolt.execute(input, _collector);
            _collector.getOutputter().ack(input);
        } catch(FailedException e) {
            if(e instanceof ReportedFailedException) {
                _collector.reportError(e);
            }
            _collector.getOutputter().fail(input);
        }
    }

    @Override
    public void cleanup() {
        _bolt.cleanup();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return _bolt.getComponentConfiguration();
    }
}
