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

package com.twitter.heron.api.bolt;

import java.util.Map;

import com.twitter.heron.api.exception.FailedException;
import com.twitter.heron.api.exception.ReportedFailedException;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;

public class BasicBoltExecutor implements IRichBolt {
  private IBasicBolt mBolt;
  private transient BasicOutputCollector mCollector;

  public BasicBoltExecutor(IBasicBolt bolt) {
    mBolt = bolt;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    mBolt.declareOutputFields(declarer);
  }


  @Override
  public void prepare(Map heronConf, TopologyContext context, OutputCollector collector) {
    mBolt.prepare(heronConf, context);
    mCollector = new BasicOutputCollector(collector);
  }

  @Override
  public void execute(Tuple input) {
    mCollector.setContext(input);
    try {
      mBolt.execute(input, mCollector);
      mCollector.getOutputter().ack(input);
    } catch (FailedException e) {
      if (e instanceof ReportedFailedException) {
        mCollector.reportError(e);
      }
      mCollector.getOutputter().fail(input);
    }
  }

  @Override
  public void cleanup() {
    mBolt.cleanup();
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return mBolt.getComponentConfiguration();
  }
}
