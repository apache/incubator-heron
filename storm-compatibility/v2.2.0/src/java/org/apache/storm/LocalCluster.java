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

package org.apache.storm;

import java.util.Map;

import org.apache.heron.simulator.Simulator;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.ConfigUtils;

@SuppressWarnings("rawtypes")
public class LocalCluster implements ILocalCluster {
  private final Simulator simulator;
  private String topologyName;
  private Map conf;
  private StormTopology topology;

  public LocalCluster() {
    this.simulator = new Simulator();
    resetFields();
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void submitTopology(String topoName,
                             Map config,
                             StormTopology stormTopology)
      throws AlreadyAliveException, InvalidTopologyException {
    assertNotAlive();

    this.topologyName = topoName;
    this.conf = config;
    this.topology = stormTopology;

    simulator.submitTopology(topoName,
        ConfigUtils.translateConfig(config),
        stormTopology.getStormTopology());
  }

  @Override
  public void killTopology(String topoName) throws NotAliveException {
    assertAlive(topoName);
    simulator.killTopology(topoName);
    resetFields();
  }

  @Override
  public void activate(String topoName) throws NotAliveException {
    assertAlive(topoName);
    simulator.activate(topoName);
  }

  @Override
  public void deactivate(String topoName) throws NotAliveException {
    assertAlive(topoName);
    simulator.deactivate(topoName);
  }

  @Override
  public void shutdown() {
    resetFields();
    simulator.shutdown();
  }

  @Override
  public String getTopologyConf(String topoName) {
    try {
      assertAlive(topoName);
      return this.topologyName;
    } catch (NotAliveException ex) {
      return null;
    }
  }

  @Override
  public StormTopology getTopology(String topoName) {
    try {
      assertAlive(topoName);
      return this.topology;
    } catch (NotAliveException ex) {
      return null;
    }
  }

  @Override
  @SuppressWarnings("rawtypes")
  public Map getState() {
    throw new RuntimeException("Heron does not support LocalCluster yet...");
  }

  private void resetFields() {
    this.topologyName = null;
    this.topology = null;
    this.conf = null;
  }

  private void assertAlive(String topoName) throws NotAliveException {
    if (this.topologyName == null || !this.topologyName.equals(topoName)) {
      throw new NotAliveException();
    }
  }

  private void assertNotAlive() throws AlreadyAliveException {
    // only one topology is allowed to run. A topology is running if the topologyName is set.
    if (this.topologyName != null) {
      throw new AlreadyAliveException();
    }
  }
}
