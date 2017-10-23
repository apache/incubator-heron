package org.apache.storm;

import com.twitter.heron.common.basics.ByteAmount;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {

    private TopologyBuilder builder;
    private Config conf;

    @SuppressWarnings( "deprecation" )
    public WordCountTopology() throws InterruptedException, InvalidTopologyException, NotAliveException, AlreadyAliveException {
        builder = new TopologyBuilder();

        builder.setSpout("sentence", new RandomSentenceSpout(),1);
        builder.setBolt("split", new SplitSentenceBolt(),1).shuffleGrouping("sentence");
        builder.setBolt("count", new WordCountBolt(),1).fieldsGrouping("split", new Fields("word"));
        conf = new Config();

        // Resource Configs
        com.twitter.heron.api.Config.setComponentRam(conf, "sentence", ByteAmount.fromMegabytes(16));
        com.twitter.heron.api.Config.setComponentRam(conf, "split", ByteAmount.fromMegabytes(16));
        com.twitter.heron.api.Config.setComponentRam(conf, "count", ByteAmount.fromMegabytes(16));
        com.twitter.heron.api.Config.setContainerCpuRequested(conf, 1);

    }

    public void execute() throws InterruptedException, InvalidTopologyException, NotAliveException, AlreadyAliveException {
        Util.runTopologyLocally(builder.createTopology(),"WordCountTopology", conf, 10);
    }

}

