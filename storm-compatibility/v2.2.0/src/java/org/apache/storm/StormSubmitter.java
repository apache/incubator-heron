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

import org.apache.heron.api.HeronSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.ConfigUtils;

/**
 * Use this class to submit topologies to run on the Storm cluster. You should run your program
 * with the "storm jar" command from the command-line, and then use this class to
 * submit your topologies.
 */
public final class StormSubmitter {

  private StormSubmitter() {
  }

  /**
   * Submits a topology to run on the cluster. A topology runs forever or until
   * explicitly killed.
   *
   * @param name the name of the storm.
   * @param stormConfig the topology-specific configuration. See {@link Config}.
   * @param topology the processing to execute.
   * @throws AlreadyAliveException if a topology with this name is already running
   * @throws InvalidTopologyException if an invalid topology was submitted
   */
  @SuppressWarnings("rawtypes")
  public static void submitTopology(
      String name,
      Map stormConfig,
      StormTopology topology) throws AlreadyAliveException, InvalidTopologyException {

    // First do topology config translation. Bolt and Spout config translation is handled
    // in their own DeclarerImpl classes.
    org.apache.heron.api.Config heronConfig = ConfigUtils.translateConfig(stormConfig);

    // Now submit a heron topology
    try {
      HeronSubmitter.submitTopology(name, heronConfig, topology.getStormTopology());
    } catch (org.apache.heron.api.exception.AlreadyAliveException e) {
      throw new AlreadyAliveException();
    } catch (org.apache.heron.api.exception.InvalidTopologyException e) {
      throw new InvalidTopologyException();
    }
  }
}
