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

package org.apache.heron.api;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.heron.api.exception.AlreadyAliveException;
import org.apache.heron.api.exception.InvalidTopologyException;
import org.apache.heron.api.exception.TopologySubmissionException;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.utils.TopologyUtils;
import org.apache.heron.api.utils.Utils;

/**
 * Use this class to submit topologies to run on the Heron cluster. You should run your program
 * with the "heron jar" command from the command-line, and then use this class to
 * submit your topologies.
 */
public final class HeronSubmitter {

  private static final Logger LOG = Logger.getLogger(HeronSubmitter.class.getName());

  private static final String TOPOLOGY_DEFINITION_SUFFIX = ".defn";
  private static final String CMD_TOPOLOGY_INITIAL_STATE = "cmdline.topology.initial.state";
  private static final String CMD_TOPOLOGY_DEFN_TEMPDIR = "cmdline.topologydefn.tmpdirectory";
  private static final String CMD_TOPOLOGY_ROLE = "cmdline.topology.role";
  private static final String CMD_TOPOLOGY_ENVIRONMENT = "cmdline.topology.environment";

  private HeronSubmitter() {
  }

  /**
   * Submits a topology to run on the cluster. A topology runs forever or until
   * explicitly killed.
   *
   * @param name the name of the topology.
   * @param heronConfig the topology-specific configuration. See {@link Config}.
   * @param topology the processing to execute.
   * @throws AlreadyAliveException if a topology with this name is already running
   * @throws InvalidTopologyException if an invalid topology was submitted
   */
  public static void submitTopology(String name, Config heronConfig, HeronTopology topology)
      throws AlreadyAliveException, InvalidTopologyException {

    Map<String, String> heronCmdOptions = getHeronCmdOptions();

    // We would read the topology initial state from arguments from heron-cli
    TopologyAPI.TopologyState initialState;
    if (heronCmdOptions.get(CMD_TOPOLOGY_INITIAL_STATE) != null) {
      initialState = TopologyAPI.TopologyState.valueOf(
          heronCmdOptions.get(CMD_TOPOLOGY_INITIAL_STATE));
    } else {
      initialState = TopologyAPI.TopologyState.RUNNING;
    }

    LOG.log(Level.FINE, "To deploy a topology in initial state {0}", initialState);

    //  add role and environment if present
    final String role = heronCmdOptions.get(CMD_TOPOLOGY_ROLE);
    if (role != null) {
      heronConfig.putIfAbsent(Config.TOPOLOGY_TEAM_NAME, role);
    }

    final String environment = heronCmdOptions.get(CMD_TOPOLOGY_ENVIRONMENT);
    if (environment != null) {
      heronConfig.putIfAbsent(Config.TOPOLOGY_TEAM_ENVIRONMENT, environment);
    }

    TopologyAPI.Topology fTopology =
        topology.setConfig(heronConfig).
            setName(name).
            setState(initialState).
            getTopology();
    TopologyUtils.validateTopology(fTopology);
    assert fTopology.isInitialized();

    submitTopologyToFile(fTopology, heronCmdOptions);
  }

  /**
   * Submits a topology to definition file
   *
   * @param fTopology the processing to execute.
   * @param heronCmdOptions the commandline options.
   * @throws TopologySubmissionException if the topology submission is failed
   */
  private static void submitTopologyToFile(TopologyAPI.Topology fTopology,
                                           Map<String, String> heronCmdOptions) {
    String dirName = heronCmdOptions.get(CMD_TOPOLOGY_DEFN_TEMPDIR);
    if (dirName == null || dirName.isEmpty()) {
      throw new TopologySubmissionException("Topology definition temp directory not specified. "
          + "Please set cmdline option: " + CMD_TOPOLOGY_DEFN_TEMPDIR);
    }

    String fileName =
        Paths.get(dirName, fTopology.getName() + TOPOLOGY_DEFINITION_SUFFIX).toString();

    try (FileOutputStream fos = new FileOutputStream(new File(fileName));
        BufferedOutputStream bos = new BufferedOutputStream(fos)) {
      byte[] topEncoding = fTopology.toByteArray();
      bos.write(topEncoding);
    } catch (IOException e) {
      throw new TopologySubmissionException("Error writing topology definition to temp directory: "
          + dirName, e);
    }
  }

  /**
   * Submits a topology to run on the cluster. A topology runs forever or until
   * explicitly killed.
   */
  // TODO add submit options
  public static String submitJar(Config config, String localJar) {
    throw new UnsupportedOperationException("submitJar functionality is unsupported");
  }

  static Map<String, String> getHeronCmdOptions() {
    return Utils.readCommandLineOpts();
  }
}
