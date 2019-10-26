/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.integration_test.topology.windowing;

import java.net.MalformedURLException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.apache.heron.api.bolt.BaseWindowedBolt;
import org.apache.heron.api.bolt.OutputCollector;
import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.api.utils.Utils;
import org.apache.heron.api.windowing.TupleWindow;
import org.apache.heron.integration_test.common.AbstractTestTopology;
import org.apache.heron.integration_test.core.TestTopologyBuilder;

public class WindowTestBase extends AbstractTestTopology {

  private static final String NUMBER_FIELD = "number";
  private static final String STRING_FIELD = "numAsStr";
  private static final String EVENT_TIME_FIELD = "eventTimeField";
  private static final String DUMMY_FIELD = "dummy";

  private BaseWindowedBolt.Count windowCountLength;
  private BaseWindowedBolt.Count slideCountInterval;
  private Duration windowDurationLength;
  private Duration slideDurationInterval;
  private Long sleepBetweenTuples;
  private boolean useEventTime = false;
  private Duration watermarkInterval;


  public String getBoltName() {
    return "VerificationBolt";
  }

  public String getSpoutName() {
    return "IncrementingSpout";
  }


  public WindowTestBase(String[] args) throws MalformedURLException {
    super(args);
  }

  public WindowTestBase withWindowLength(Duration duration) {
    this.windowDurationLength = duration;
    return this;
  }

  public WindowTestBase withWindowLength(BaseWindowedBolt.Count count) {
    this.windowCountLength = count;
    return this;
  }

  public WindowTestBase withSlidingInterval(Duration duration) {
    this.slideDurationInterval = duration;
    return this;
  }

  public WindowTestBase withSlidingInterval(BaseWindowedBolt.Count count) {
    this.slideCountInterval = count;
    return this;
  }

  public WindowTestBase withSleepBetweenTuples(long millis) {
    this.sleepBetweenTuples = millis;
    return this;
  }

  public WindowTestBase useEventTime() {
    this.useEventTime = true;
    return this;
  }

  @SuppressWarnings("HiddenField")
  public WindowTestBase withWatermarkInterval(Duration watermarkInterval) {
    this.watermarkInterval = watermarkInterval;
    return this;
  }

  @Override
  protected TestTopologyBuilder buildTopology(TestTopologyBuilder builder) {
    builder.setSpout(getSpoutName(), new IncrementingSpout()
        .withSleepBetweenTuples(sleepBetweenTuples), 1);

    BaseWindowedBolt bolt = null;
    if (this.windowCountLength != null) {
      if (this.slideCountInterval != null) {
        bolt = new VerificationBolt()
            .withWindow(this.windowCountLength, this.slideCountInterval);
      } else {
        bolt = new VerificationBolt()
            .withTumblingWindow(this.windowCountLength);
      }
    }

    if (this.windowDurationLength != null) {
      if (this.slideDurationInterval != null) {
        bolt = new VerificationBolt()
            .withWindow(this.windowDurationLength, this.slideDurationInterval);
      } else {
        bolt = new VerificationBolt()
            .withTumblingWindow(this.windowDurationLength);
      }
    }

    if (this.useEventTime) {
      bolt.withTimestampField(EVENT_TIME_FIELD);
    }

    if (this.watermarkInterval != null) {
      bolt.withWatermarkInterval(this.watermarkInterval);
    }
    builder.setBolt(getBoltName(), bolt, 1, false)
        .shuffleGrouping(getSpoutName());
    return builder;
  }

  public static class IncrementingSpout extends BaseRichSpout {
    private static final Logger LOG = Logger.getLogger(IncrementingSpout.class.getName());
    private static final long serialVersionUID = -6171170228097868632L;
    private SpoutOutputCollector collector;
    private static int currentNum;
    private static Random rng = new Random();
    private String componentId;
    private Long sleepBetweenTuples = null;
    // In millis
    private long currentTime = 1504573536000L;

    public IncrementingSpout withSleepBetweenTuples(long millis) {
      this.sleepBetweenTuples = millis;
      return this;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(NUMBER_FIELD, STRING_FIELD, EVENT_TIME_FIELD));
    }

    @Override
    @SuppressWarnings("HiddenField")
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector
        collector) {
      componentId = context.getThisComponentId();
      this.collector = collector;
    }

    @Override
    public void nextTuple() {
      if (sleepBetweenTuples == null) {
        Utils.sleep(rng.nextInt(10));
      } else {
        Utils.sleep(sleepBetweenTuples);
      }
      currentNum++;
      final String numAsStr = "str(" + currentNum + ")str";
      final Values tuple = new Values(currentNum, numAsStr, currentTime += 1000);
      LOG.info("Time = " + System.currentTimeMillis()
          + " Component = " + componentId + " Tuple = " + tuple.toString());

      collector.emit(tuple, currentNum);
    }

    @Override
    public void ack(Object msgId) {
      LOG.info("Received ACK for msgId : " + msgId);
    }

    @Override
    public void fail(Object msgId) {
      LOG.info("Received FAIL for msgId : " + msgId);
    }
  }

  public static class VerificationBolt extends BaseWindowedBolt {
    private static final long serialVersionUID = -6067634845003700125L;
    private OutputCollector collector;
    private String componentId;

    private static int windowCount = 0;


    private static final Logger LOG = Logger.getLogger(VerificationBolt.class.getName());


    @Override
    @SuppressWarnings("HiddenField")
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector
        collector) {
      componentId = context.getThisComponentId();
      this.collector = collector;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(TupleWindow inputWindow) {
      windowCount++;
      List<Tuple> tuplesInWindow = inputWindow.get();
      List<Tuple> newTuples = inputWindow.getNew();
      List<Tuple> expiredTuples = inputWindow.getExpired();
      LOG.info("tuplesInWindow.size() = " + tuplesInWindow.size());
      LOG.info("newTuples.size() = " + newTuples.size());
      LOG.info("expiredTuples.size() = " + expiredTuples.size());

      JSONObject jsonObject = new JSONObject();
      JSONArray jsonArray = new JSONArray();
      JSONObject tmp = new JSONObject();
      tmp.put("tuplesInWindow", tuplesToListString(tuplesInWindow));
      jsonArray.add(tmp);
      tmp = new JSONObject();
      tmp.put("newTuples", tuplesToListString(newTuples));
      jsonArray.add(tmp);
      tmp = new JSONObject();
      tmp.put("expiredTuples", tuplesToListString(expiredTuples));
      jsonArray.add(tmp);
      jsonObject.put(windowCount, jsonArray);
      LOG.info("Component = " + componentId + " Window Count = " + windowCount
          + " tuplesInWindow = " + jsonArray.toJSONString());

      collector.emit(new Values(jsonObject.toJSONString()));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(DUMMY_FIELD));
    }
  }

  public static List<String> tuplesToListString(List<Tuple> tuples) {
    List<String> tmp = new LinkedList<>();
    for (Tuple tuple : tuples) {
      tmp.add(tuple.getValue(0).toString());
    }
    return tmp;
  }
}
