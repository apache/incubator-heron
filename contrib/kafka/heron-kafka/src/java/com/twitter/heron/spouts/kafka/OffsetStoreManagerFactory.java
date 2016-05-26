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

package com.twitter.heron.spouts.kafka;

import com.twitter.heron.spouts.kafka.common.IOffsetStoreManager;

@SuppressWarnings("rawtypes")
public class OffsetStoreManagerFactory {

  public static final String KAFKA_STORE = "kafka";

  private SpoutConfig spoutConfig;

  public OffsetStoreManagerFactory(SpoutConfig spoutConfig) {
    this.spoutConfig = spoutConfig;
  }

  public IOffsetStoreManager get() {
    if (spoutConfig.offsetStore != null && !spoutConfig.offsetStore.isEmpty()) {
      if (spoutConfig.offsetStore.equalsIgnoreCase(KAFKA_STORE)) {
        return new KafkaOffsetStoreManager(spoutConfig);
      } else {
        throw new RuntimeException("Unknown offset store specified: " + spoutConfig.offsetStore);
      }
    }

    throw new RuntimeException("Offset store should be specified");
  }
}
