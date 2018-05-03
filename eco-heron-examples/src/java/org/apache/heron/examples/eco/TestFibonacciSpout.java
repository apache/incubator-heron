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

package org.apache.heron.examples.eco;

import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.api.utils.Utils;

@SuppressWarnings({"serial", "rawtypes", "HiddenField"})
public class TestFibonacciSpout extends BaseRichSpout {
  private static final Logger LOG = Logger.getLogger(TestFibonacciSpout.class.getName());
  private TestPropertyHolder holder;
  private SpoutOutputCollector collector;

  public TestFibonacciSpout(TestPropertyHolder holder) {
    this.holder = holder;
  }

  @Override
  public void open(Map<String, Object> conf, TopologyContext context,
                   SpoutOutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void nextTuple() {
    Utils.sleep(100);
    final int[] words = new int[] {0, 1, 2, 3, 5, 8, 13, 21, 34};
    final Random rand = new Random();
    final int number = words[rand.nextInt(words.length)];
    final String property = holder.getProperty();
    final int numberProperty = holder.getNumberProperty();
    final String publicProperty = holder.publicProperty;
    LOG.info("Constructor Args: " + property);
    LOG.info("Property set by setter: " + numberProperty);
    LOG.info("Property set by public field: " + publicProperty);
    LOG.info("Emitting: number " + number);
    collector.emit(new Values(number));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("number"));
  }
}
