//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License
package com.twitter.heron.examples;

import java.util.Arrays;
import java.util.Collections;

import org.apache.storm.Config;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.trident.topology.TridentTopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import org.apache.storm.StormSubmitter;

/**
 *
 */
public class TridentWordCountTopology {

  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
    if (args.length < 1) {
      throw new RuntimeException("Specify topology name");
    }

    int parallelism = 1;
    if (args.length > 1) {
      parallelism = Integer.parseInt(args[1]);
    }

    @SuppressWarnings("unchecked")
    FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                   new Values("the cow jumped over the moon"),
                   new Values("the man went to the store and bought some candy"),
                   new Values("four score and seven years ago"),
                   new Values("how many apples can you eat"));
    spout.setCycle(true);

    // This spout cycles through that set of sentences over and over to produce the sentence stream.
    // Here's the code to do the streaming word count part of the computation:
    TridentTopology topology = new TridentTopology();
    TridentState wordCounts =
         topology.newStream("spout1", spout)
           .each(new Fields("sentence"), new Split(), new Fields("word"))
           .groupBy(new Fields("word"))
           .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
           .parallelismHint(parallelism);

    Config conf = new Config();
    conf.setDebug(true);

    // TODO: for some reason this is automatically added to spouts but not bolts...
    conf.put(Config.TOPOLOGY_KRYO_REGISTER, Collections.singletonList(
        "org.apache.storm.trident.topology.TransactionAttempt"));
    conf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, Collections.singletonList("localhost"));
    conf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, 2181);
    conf.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, "/transaction_root");
    conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, 5000);
    conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 5000);
    conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 5);
    conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 5000);
    conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING, 5000);
    //Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS
    conf.put("topology.trident.batch.emit.interval.millis", 5000);

    StormTopology stormTopology = topology.build();
    StormSubmitter.submitTopology(args[0], conf, stormTopology);
  }
}
