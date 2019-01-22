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

package org.apache.heron.statemgr.zookeeper;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.heron.common.basics.Pair;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.utils.NetworkUtils;

public final class ZkUtils {
  public static final String LOCAL_HOST = "127.0.0.1";

  private static final Logger LOG = Logger.getLogger(ZkUtils.class.getName());

  private ZkUtils() {

  }

  /**
   * Setup the tunnel if needed
   *
   * @param config basing on which we setup the tunnel process
   * @return Pair of (zk_format_connectionString, List of tunneled processes)
   */
  public static Pair<String, List<Process>> setupZkTunnel(Config config,
                                                          NetworkUtils.TunnelConfig tunnelConfig) {
    // Remove all spaces
    String connectionString = Context.stateManagerConnectionString(config).replaceAll("\\s+", "");

    List<Pair<InetSocketAddress, Process>> ret = new ArrayList<>();

    // For zookeeper, connection String can be a list of host:port, separated by comma
    String[] endpoints = connectionString.split(",");
    for (String endpoint : endpoints) {
      InetSocketAddress address = NetworkUtils.getInetSocketAddress(endpoint);

      // Get the tunnel process if needed
      Pair<InetSocketAddress, Process> pair =
          NetworkUtils.establishSSHTunnelIfNeeded(
              address, tunnelConfig, NetworkUtils.TunnelType.PORT_FORWARD);

      ret.add(pair);
    }

    // Construct the new ConnectionString and tunnel processes
    StringBuilder connectionStringBuilder = new StringBuilder();
    List<Process> tunnelProcesses = new ArrayList<>();

    String delim = "";
    for (Pair<InetSocketAddress, Process> pair : ret) {
      // Join the list of String with comma as delim
      if (pair.first != null) {
        connectionStringBuilder.append(delim).
            append(pair.first.getHostName()).append(":").append(pair.first.getPort());
        delim = ",";

        // If tunneled
        if (pair.second != null) {
          tunnelProcesses.add(pair.second);
        }
      }
    }

    String newConnectionString = connectionStringBuilder.toString();
    return new Pair<String, List<Process>>(newConnectionString, tunnelProcesses);
  }
}
