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

package backtype.storm.spout;

import java.util.Map;

import backtype.storm.Config;

public class SleepSpoutWaitStrategy implements ISpoutWaitStrategy {

  long sleepMillis;

  @Override
  public void prepare(Map<String, Object> conf) {
    sleepMillis = ((Number) conf.get(Config.TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS)).longValue();
  }

  @Override
  public void emptyEmit(long streak) {
    try {
      Thread.sleep(sleepMillis);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
