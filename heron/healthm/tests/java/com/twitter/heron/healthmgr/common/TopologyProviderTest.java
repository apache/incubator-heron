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

package com.twitter.heron.healthmgr.common;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.generated.TopologyAPI.Topology;
import com.twitter.heron.spi.utils.TopologyTests;

import static org.junit.Assert.assertEquals;

public class TopologyProviderTest {
  @Test
  public void providesBoltNames() {
    Config topologyConfig = new Config();
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", 1);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt-1", 1);
    bolts.put("bolt-2", 1);

    final Topology topology =
        TopologyTests.createTopology("topology", topologyConfig, spouts, bolts);

    TopologyProvider topologyProvider = new TopologyProvider();
    topologyProvider.setTopology(topology);

    String[] boltNames = topologyProvider.getBoltNames();
    assertEquals(2, boltNames.length);
    assertEquals(2, bolts.size());
    for (String boltName : boltNames) {
      bolts.remove(boltName);
    }
    assertEquals(0, bolts.size());
  }
}
