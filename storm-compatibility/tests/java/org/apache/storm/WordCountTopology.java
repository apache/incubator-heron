// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm;

import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.twitter.heron.common.basics.ByteAmount;

public class WordCountTopology {

  public static final ByteAmount RAM_IN_BYTES = ByteAmount.fromMegabytes(16);
  private TopologyBuilder builder;
  private Config conf;

  @SuppressWarnings("deprecation")
  public WordCountTopology() throws InterruptedException, InvalidTopologyException,
      NotAliveException,
      AlreadyAliveException {
    builder = new TopologyBuilder();

    builder.setSpout("sentence", new RandomSentenceSpout(), 1);
    builder.setBolt("split", new SplitSentenceBolt(), 1).shuffleGrouping("sentence");
    builder.setBolt("count", new WordCountBolt(), 1)
        .fieldsGrouping("split", new Fields("word"));
    conf = new Config();

    // Resource Configs
    com.twitter.heron.api.Config.setComponentRam(conf, "sentence", RAM_IN_BYTES);
    com.twitter.heron.api.Config.setComponentRam(conf, "split", RAM_IN_BYTES);
    com.twitter.heron.api.Config.setComponentRam(conf, "count", RAM_IN_BYTES);
    com.twitter.heron.api.Config.setContainerCpuRequested(conf, 1);

  }

  public void execute() throws InterruptedException, InvalidTopologyException, NotAliveException,
      AlreadyAliveException {
    Util.runTopologyLocally(builder.createTopology(), "WordCountTopology", conf, 10);
  }

}

