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

package org.apache.heron.apiserver.actions;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.scheduler.SubmitterMain;
import org.apache.heron.spi.common.Config;

public class SubmitTopologyAction implements Action {

  private final Config configuration;
  private final TopologyAPI.Topology topology;

  SubmitTopologyAction(Config configuration, TopologyAPI.Topology topology) {
    this.configuration = configuration;
    this.topology = topology;
  }

  @Override
  public void execute() {
    final SubmitterMain submitter = new SubmitterMain(configuration, topology);
    submitter.submitTopology();
  }
}
