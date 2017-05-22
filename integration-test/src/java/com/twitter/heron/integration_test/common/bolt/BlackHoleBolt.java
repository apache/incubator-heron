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
package com.twitter.heron.integration_test.common.bolt;

import java.util.logging.Logger;

import com.twitter.heron.api.bolt.BaseBasicBolt;
import com.twitter.heron.api.bolt.BasicOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;

public class BlackHoleBolt extends BaseBasicBolt {
  private static final long serialVersionUID = 8606341173199587030L;
  private static final Logger LOG = Logger.getLogger(IdentityBolt.class.getName());
  private Fields fields;

  public BlackHoleBolt(Fields fields) {
    this.fields = fields;
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    LOG.info("This bolt is a black hole emitting nothing");
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(fields);
  }
}
