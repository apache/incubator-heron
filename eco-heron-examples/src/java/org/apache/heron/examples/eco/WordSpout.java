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

import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Values;

@SuppressWarnings("HiddenField")
public class WordSpout extends BaseRichSpout {
  private static final long serialVersionUID = 4322775001819135036L;

  private static final int ARRAY_LENGTH = 128 * 1024;
  private static final int WORD_LENGTH = 20;

  private final String[] words = new String[ARRAY_LENGTH];

  private final Random rnd = new Random(31);

  private SpoutOutputCollector collector;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("word"));
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void open(Map map, TopologyContext topologyContext,
                   SpoutOutputCollector spoutOutputCollector) {
    System.out.println("open spout");
    RandomString randomString = new RandomString(WORD_LENGTH);

    for (int i = 0; i < ARRAY_LENGTH; i++) {
      words[i] = randomString.nextString();
    }

    collector = spoutOutputCollector;
  }

  @Override
  public void nextTuple() {
    System.out.println("next tuple");
    int nextInt = rnd.nextInt(ARRAY_LENGTH);
    collector.emit(new Values(words[nextInt]));
  }
}
