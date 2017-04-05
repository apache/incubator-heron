// Copyright 2016 Microsoft. All rights reserved.
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

import java.util.ArrayList;

import javax.inject.Provider;
import javax.inject.Singleton;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.generated.TopologyAPI.Topology;

/**
 * A topology may be updated after initial deployment. This provider is used to provide the latest
 * version to any dependent components.
 */
@Singleton
public class TopologyProvider implements Provider<Topology> {
  private Topology topology;

  @Override
  public Topology get() {
    return topology;
  }

  public String[] getBoltNames() {
    Topology topology = get();
    ArrayList<String> boltNames = new ArrayList<>();
    for (TopologyAPI.Bolt bolt : topology.getBoltsList()) {
      boltNames.add(bolt.getComp().getName());
    }

    return boltNames.toArray(new String[boltNames.size()]);
  }

  public void setTopology(Topology topology) {
    this.topology = topology;
  }
}