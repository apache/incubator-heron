//  Copyright 2018 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.eco.submit;


import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.HeronTopology;

public class EcoSubmitter {

  public void submitStormTopology(String topologyName,
                                  Config topologyConfig, StormTopology topology)
      throws org.apache.storm.generated.AlreadyAliveException,
      org.apache.storm.generated.InvalidTopologyException {
    StormSubmitter.submitTopology(topologyName, topologyConfig, topology);
  }

  public void submitHeronTopology(String topologyName,
                                  Config topologyConfig, HeronTopology topology)
      throws com.twitter.heron.api.exception.AlreadyAliveException,
      com.twitter.heron.api.exception.InvalidTopologyException {
    HeronSubmitter.submitTopology(topologyName, topologyConfig, topology);
  }
}
