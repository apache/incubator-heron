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

package org.apache.heron.eco.submit;

import org.apache.heron.api.Config;
import org.apache.heron.api.HeronSubmitter;
import org.apache.heron.api.HeronTopology;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;

public class EcoSubmitter {

  public void submitStormTopology(String topologyName,
                                  Config topologyConfig, StormTopology topology)
      throws org.apache.storm.generated.AlreadyAliveException,
      org.apache.storm.generated.InvalidTopologyException {
    StormSubmitter.submitTopology(topologyName, topologyConfig, topology);
  }

  public void submitHeronTopology(String topologyName,
                                  Config topologyConfig, HeronTopology topology)
      throws org.apache.heron.api.exception.AlreadyAliveException,
      org.apache.heron.api.exception.InvalidTopologyException {
    HeronSubmitter.submitTopology(topologyName, topologyConfig, topology);
  }
}
