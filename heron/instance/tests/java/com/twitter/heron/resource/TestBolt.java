package com.twitter.heron.resource;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.common.basics.SingletonRegistry;

import org.junit.Ignore;

/**
 * A Bolt used for unit test, it will execute and modify some singletons' value:
 * 1. It will tuple.getString(0) and append to the singleton "received-string-list"
 * 2. It will increment singleton "execute-count" when it executes once
 * 3. It will ack the tuple and increment singleton Constants.ACK_COUNT when # of tuples executed is odd
 * 4. It will fail the tuple and increment singleton Constants.FAIL_COUNT when # of tuples executed is even
 */
@Ignore
public class TestBolt extends BaseRichBolt {
  OutputCollector outputCollector;
  int tupleExecuted = 0;

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.outputCollector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    AtomicInteger ackCount = (AtomicInteger) SingletonRegistry.INSTANCE.getSingleton(Constants.ACK_COUNT);
    AtomicInteger failCount = (AtomicInteger) SingletonRegistry.INSTANCE.getSingleton(Constants.FAIL_COUNT);
    AtomicInteger tupleExecutedCount = (AtomicInteger) SingletonRegistry.INSTANCE.getSingleton("execute-count");
    StringBuilder receivedStrings = (StringBuilder) SingletonRegistry.INSTANCE.getSingleton("received-string-list");

    if (receivedStrings != null) {
      receivedStrings.append(tuple.getString(0));
    }

    if (tupleExecutedCount != null) {
      tupleExecutedCount.getAndIncrement();
    }
    if ((tupleExecuted & 1) == 0) {
      outputCollector.ack(tuple);
      if (ackCount != null) {
        ackCount.getAndIncrement();
      }
    } else {
      outputCollector.fail(tuple);
      if (failCount != null) {
        failCount.getAndIncrement();
      }
    }
    tupleExecuted++;
    outputCollector.emit(new Values(tuple.getString(0)));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("word"));
  }
}
