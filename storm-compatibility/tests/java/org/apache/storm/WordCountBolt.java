package org.apache.storm;

import backtype.storm.tuple.Values;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("serial")
public class WordCountBolt extends BaseBasicBolt {

    private static final int EMIT_FREQUENCY = 3;
    private Map<String, Integer> counts = new HashMap<>();
    private Integer emitFrequency;

    public WordCountBolt(){
        this(EMIT_FREQUENCY);
    }

    public WordCountBolt(int emitFrequency){
        this.emitFrequency = emitFrequency;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency);
        return conf;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println("Execute Entry");
        if(Util.isTickTuple(tuple)) {
            for(String word : counts.keySet()) {
                Integer count = counts.get(word);
                collector.emit(new Values(word, count));
                System.out.println(String.format("Emitting a count of (%d) for word (%s)", count, word));
            }
        } else {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            count++;
            counts.put(word, count);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}