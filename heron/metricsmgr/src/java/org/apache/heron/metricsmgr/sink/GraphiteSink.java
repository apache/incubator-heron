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

package org.apache.heron.metricsmgr.sink;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.heron.common.basics.TypeUtils;
import org.apache.heron.spi.metricsmgr.metrics.MetricsInfo;
import org.apache.heron.spi.metricsmgr.metrics.MetricsRecord;
import org.apache.heron.spi.metricsmgr.sink.IMetricsSink;
import org.apache.heron.spi.metricsmgr.sink.SinkContext;

/**
 * A metrics sink that writes to a Graphite server
 * <p>
 * When exceptions are occurred to access Graphite Server, close(...) would be invoked.
 * And re-connecting would happen automatically in next write(...).
 * <p>
 * Only when too many re-connections occurred, we would throw a RuntimeException.
 * <p>
 * TODO -- This GraphiteSink is just a template.
 * TODO -- processRecord(MetricsRecord record) needs changes to apply the specific scenarios
 * TODO -- complete integration test is needed.
 */
public class GraphiteSink implements IMetricsSink {
  private static final Logger LOG = Logger.getLogger(GraphiteSink.class.getName());

  private static final int DEFAULT_MAX_CONNECTION_FAILURES = 5;

  // These configs would be read from sink-configs.yaml
  private static final String SERVER_HOST_KEY = "graphite_host";
  private static final String SERVER_PORT_KEY = "graphite_port";
  private static final String METRICS_PREFIX = "metrics_prefix";
  private static final String SERVER_MAX_RECONNECT_ATTEMPTS = "server_max_reconnect-attempts";

  private String metricsPrefix = null;
  private Graphite graphite = null;
  private String topologyName = null;

  @Override
  public void init(Map<String, Object> conf, SinkContext context) {
    LOG.info("Configuration: " + conf.toString());

    // Get Graphite host configurations.
    final String serverHost = (String) conf.get(SERVER_HOST_KEY);
    final int serverPort = TypeUtils.getInteger(conf.get(SERVER_PORT_KEY));

    // Safe check
    if (conf.get(SERVER_HOST_KEY) == null
        || conf.get(SERVER_PORT_KEY) == null) {
      throw new IllegalArgumentException("Server's host or port could not fetch from config");
    }

    int maxServerReconnectAttempts = conf.get(SERVER_HOST_KEY) == null
        ? DEFAULT_MAX_CONNECTION_FAILURES
        : TypeUtils.getInteger(conf.get(SERVER_MAX_RECONNECT_ATTEMPTS));

    // Get Graphite metrics graph prefix.
    metricsPrefix = (String) conf.get(METRICS_PREFIX);
    if (metricsPrefix == null) {
      metricsPrefix = "";
    }

    topologyName = context.getTopologyName();

    graphite = new Graphite(serverHost, serverPort, maxServerReconnectAttempts);
    graphite.connect();
  }

  @Override
  public void processRecord(MetricsRecord record) {
    StringBuilder lines = new StringBuilder();
    StringBuilder metricsPathPrefix = new StringBuilder();

    // Configure the hierarchical place to display the graph.
    // The metricsPathPrefix would look like:
    // {metricsPrefix}.{topologyName}.{host:port/componentName/instanceId}
    metricsPathPrefix.append(metricsPrefix).append(".")
        .append(topologyName).append(".").append(record.getSource());

    // The record timestamp is in milliseconds while Graphite expects an epoc time in seconds.
    long timestamp = record.getTimestamp() / 1000L;

    // Collect data points.
    // Every data point would look like:
    // {metricsPrefix}.{topologyName}.{host:port/componentName/instanceId}.{metricName}
    //    {metricValue} {timestamp} \n
    for (MetricsInfo metricsInfo : record.getMetrics()) {
      lines.append(
          metricsPathPrefix.toString() + "."
              + metricsInfo.getName().replace(' ', '.')).append(" ")
          .append(metricsInfo.getValue()).append(" ").append(timestamp)
          .append("\n");
    }

    try {
      graphite.write(lines.toString());
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Error sending metrics to Graphite. Dropping messages...", e);

      // Here we do not invoke GraphiteSink.close(), since:
      // 1. We just want to close the connection to GraphiteServer
      // 2. GraphiteSink.close() would contain more cleaning logic for the whole sink
      try {
        graphite.close();
      } catch (IOException e1) {
        LOG.log(Level.SEVERE, "Error closing connection to Graphite", e1);
      }
    }
  }

  @Override
  public void flush() {
    try {
      graphite.flush();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Error flushing metrics to Graphite. Dropping messages...", e);

      // Here we do not invoke GraphiteSink.close(), since:
      // 1. We just want to close the connection to GraphiteServer
      // 2. GraphiteSink.close() would contain more cleaning logic for the whole sink
      try {
        graphite.close();
      } catch (IOException e1) {
        LOG.log(Level.SEVERE, "Error closing connection to Graphite", e1);
      }
    }
  }

  @Override
  public void close() {
    try {
      graphite.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Error closing connection to Graphite", e);
    }
  }

  public static class Graphite {
    private final String serverHost;
    private final int serverPort;
    private final int maxConnectionFailures;

    private Writer writer = null;
    private Socket socket = null;
    private int connectionFailures = 0;

    public Graphite(String serverHost, int serverPort, int maxConnectionFailure) {
      this.serverHost = serverHost;
      this.serverPort = serverPort;
      this.maxConnectionFailures = maxConnectionFailure;
    }

    public void connect() {
      if (isConnected()) {
        throw new RuntimeException("Already connected to Graphite");
      }
      if (tooManyConnectionFailures()) {
        // throw exceptions when we try with too many connection failures.
        throw new RuntimeException(
            String.format("Too many connection failures to %s:%d, would not try to connect again.",
                serverHost, serverPort));
      }
      try {
        // Open a connection to Graphite server.
        socket = new Socket(serverHost, serverPort);
        writer = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
      } catch (IOException e) {
        connectionFailures++;
        if (tooManyConnectionFailures()) {
          // first time when connection limit reached, report to logs
          LOG.severe("Too many connection failures, would not try to connect again.");
        }
        LOG.log(Level.SEVERE,
            String.format("Error creating connection, %s:%d", serverHost, serverPort),
            e);
      }
    }

    public void write(String msg) throws IOException {
      if (!isConnected()) {
        connect();
      }
      if (isConnected()) {
        writer.write(msg);
      }
    }

    public void flush() throws IOException {
      if (isConnected()) {
        writer.flush();
      }
    }

    public boolean isConnected() {
      return socket != null && socket.isConnected() && !socket.isClosed();
    }

    public void close() throws IOException {
      try {
        if (writer != null) {
          writer.close();
        }
      } catch (IOException ex) {
        if (socket != null) {
          socket.close();
        }
      } finally {
        socket = null;
        writer = null;
      }
    }

    private boolean tooManyConnectionFailures() {
      return connectionFailures > maxConnectionFailures;
    }

  }
}
