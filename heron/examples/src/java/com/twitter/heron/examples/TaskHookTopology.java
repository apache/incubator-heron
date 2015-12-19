package com.twitter.heron.examples;


import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.hooks.ITaskHook;
import com.twitter.heron.api.hooks.info.BoltAckInfo;
import com.twitter.heron.api.hooks.info.BoltExecuteInfo;
import com.twitter.heron.api.hooks.info.BoltFailInfo;
import com.twitter.heron.api.hooks.info.EmitInfo;
import com.twitter.heron.api.hooks.info.SpoutAckInfo;
import com.twitter.heron.api.hooks.info.SpoutFailInfo;
import com.twitter.heron.api.metric.GlobalMetrics;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.utils.Utils;

public class TaskHookTopology {
  public static class TestTaskHook implements ITaskHook {
    private static final long N = 10000;
    private long emitted = 0;
    private long spoutAcked = 0;
    private long spoutFailed = 0;
    private long boltExecuted = 0;
    private long boltAcked = 0;
    private long boltFailed = 0;

    private String constructString;

    public TestTaskHook() {
      this.constructString = "This TestTaskHook is constructed with no parameters";
    }

    public TestTaskHook(String constructString) {
      this.constructString = constructString;
    }

    @Override
    public void prepare(Map conf, TopologyContext context) {
      GlobalMetrics.incr("hook_prepare");
      System.out.println(constructString);
      System.out.println("prepare() is invoked in hook");
      System.out.println(conf);
      System.out.print(context);
    }

    @Override
    public void cleanup() {
      GlobalMetrics.incr("hook_cleanup");
      System.out.println("clean() is invoked in hook");
    }

    @Override
    public void emit(EmitInfo info) {
      GlobalMetrics.incr("hook_emit");
      ++emitted;
      if (emitted % N == 0) {
        System.out.println("emit() is invoked in hook");
        System.out.println(info.getValues());
        System.out.println(info.getTaskId());
        System.out.println(info.getStream());
        System.out.println(info.getOutTasks());
      }
    }

    @Override
    public void spoutAck(SpoutAckInfo info) {
      GlobalMetrics.incr("hook_spoutAck");
      ++spoutAcked;
      if (spoutAcked % N == 0) {
        System.out.println("spoutAck() is invoked in hook");
        System.out.println(info.getCompleteLatencyMs());
        System.out.println(info.getMessageId());
        System.out.println(info.getSpoutTaskId());
      }
    }

    @Override
    public void spoutFail(SpoutFailInfo info) {
      GlobalMetrics.incr("hook_spoutFail");
      ++spoutFailed;
      if (spoutFailed % N == 0) {
        System.out.println("spoutFail() is invoked in hook");
        System.out.println(info.getFailLatencyMs());
        System.out.println(info.getMessageId());
        System.out.println(info.getSpoutTaskId());
      }
    }

    @Override
    public void boltExecute(BoltExecuteInfo info) {
      GlobalMetrics.incr("hook_boltExecute");
      ++boltExecuted;
      if (boltExecuted % N == 0) {
        System.out.println("boltExecute() is invoked in hook");
        System.out.println(info.getExecuteLatencyMs());
        System.out.println(info.getExecutingTaskId());
        System.out.println(info.getTuple());
      }
    }

    @Override
    public void boltAck(BoltAckInfo info) {
      GlobalMetrics.incr("hook_boltAck");
      ++boltAcked;
      if (boltAcked % N == 0) {
        System.out.println("boltAck() is invoked in hook");
        System.out.println(info.getAckingTaskId());
        System.out.println(info.getProcessLatencyMs());
        System.out.println(info.getTuple());
      }
    }

    @Override
    public void boltFail(BoltFailInfo info) {
      GlobalMetrics.incr("hook_boltFail");
      ++boltFailed;
      if (boltFailed % N == 0) {
        System.out.println("boltFail() is invoked in hook");
        System.out.println(info.getFailingTaskId());
        System.out.println(info.getFailLatencyMs());
        System.out.println(info.getTuple());
      }
    }
  }

  public static class AckingTestWordSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    String[] words;
    Random rand;

    public AckingTestWordSpout() {
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      _collector = collector;
      words = new String[]{"nathan", "mike", "jackson", "golda", "bertels"};
      rand = new Random();

      // Add task hook dynamically
      context.addTaskHook(new TestTaskHook("This TestTaskHook is constructed by AckingTestWordSpout"));
    }

    public void close() {
    }

    public void nextTuple() {
      // We explicitly slow down the spout to avoid the stream mgr to be the bottleneck
      Utils.sleep(1);
      final String word = words[rand.nextInt(words.length)];
      // To enable acking, we need to emit tuple with MessageId, which is an object
      _collector.emit(new Values(word), word);
    }

    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  public static class CountBolt extends BaseRichBolt {
    OutputCollector _collector;
    long nItems;
    long startTime;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      nItems = 0;
      startTime = System.currentTimeMillis();

      // Add task hook dynamically
      context.addTaskHook(new TestTaskHook("This TestTaskHook is constructed by CountBolt"));

    }

    @Override
    public void execute(Tuple tuple) {
      // We need to ack a tuple when we consider it is done successfully
      // Or we could fail it by invoking _collector.fail(tuple)
      // If we do not do the ack or fail explicitly
      // After the MessageTimeout Seconds, which could be set in Config, the spout will fail this tuple
      ++nItems;
      if (nItems % 10000 == 0) {
        long latency = System.currentTimeMillis() - startTime;
        System.out.println("Done " + nItems + " in " + latency);
        GlobalMetrics.incr("selected_items");
        // Here we explicitly forget to do the ack or fail
        // It would trigger fail on this tuple on spout end after MessageTimeout Seconds
      } else if (nItems % 2 == 0) {
        _collector.fail(tuple);
      } else {
        _collector.ack(tuple);
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new RuntimeException("Specify topology name");
    }
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", new AckingTestWordSpout(), 2);
    builder.setBolt("count", new CountBolt(), 2)
        .shuffleGrouping("word");

    Config conf = new Config();
    conf.setDebug(true);
    // Put an arbitrary large number here if you don't want to slow the topology down
    conf.setMaxSpoutPending(1000 * 1000 * 1000);
    // To enable acking, we need to setEnableAcking true
    conf.setEnableAcking(true);
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");

    // Set the task hook
    List<String> taskHooks = new LinkedList<String>();
    taskHooks.add("com.twitter.heron.examples.TaskHookTopology$TestTaskHook");
    conf.setAutoTaskHooks(taskHooks);

    conf.setNumStmgrs(1);
    HeronSubmitter.submitTopology(args[0], conf, builder.createTopology());
  }
}
