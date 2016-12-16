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

package com.twitter.heron.metricsmgr.sink;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import com.twitter.heron.common.basics.TypeUtils;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsRecord;
import com.twitter.heron.spi.metricsmgr.sink.IMetricsSink;
import com.twitter.heron.spi.metricsmgr.sink.SinkContext;


/**
 * A metrics sink that publishes metrics on a http endpoint
 */
public class WebSink implements IMetricsSink {
  private static final Logger LOG = Logger.getLogger(WebSink.class.getName());
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final int HTTP_STATUS_OK = 200;

  // Metrics will be published on http://host:port/path, the port
  private static final String KEY_PORT = "port";

  // If you want to specify a file from which to read the port instead of
  // supplying it directly
  private static final String KEY_PORT_FILE = "port-file";

  // The path
  private static final String KEY_PATH = "path";

  // If flat-metrics is false, the metrics will be grouped by the metrics
  // source source,
  // if flat-metrics is true, the metric names will be prefixed by the source
  private static final String KEY_FLAT_METRICS = "flat-metrics";

  // Include the topology name in the metric name
  private static final String KEY_INCLUDE_TOPOLOGY_NAME = "include-topology-name";

  // Maximum number of metrics getting served
  private static final String KEY_METRICS_CACHE_MAX_SIZE = "metrics-cache-max-size";

  // Time To Live before a metric gets evicted from the cache
  private static final String KEY_METRICS_CACHE_TTL_SEC = "metrics-cache-ttl-sec";

  // This is the cache that is used to serve the metrics
  protected Cache<String, Object> metricsCache;

  private HttpServer httpServer;
  private boolean isFlatMetrics = true;
  private boolean includeTopologyName = true;
  private String topologyName;

  @Override
  public void init(Map<String, Object> conf, SinkContext context) {
    String path = (String) conf.get(KEY_PATH);
    String portFile = (String) conf.get(KEY_PORT_FILE);
    isFlatMetrics = TypeUtils.getBoolean(conf.getOrDefault(KEY_FLAT_METRICS, true));
    includeTopologyName = TypeUtils.getBoolean(
            conf.getOrDefault(KEY_INCLUDE_TOPOLOGY_NAME, false));
    long cacheMaxSize = TypeUtils.getLong(conf.getOrDefault(KEY_METRICS_CACHE_MAX_SIZE, 1000000));
    long cacheTtlSec = TypeUtils.getLong(conf.getOrDefault(KEY_METRICS_CACHE_TTL_SEC, 600));

    metricsCache = CacheBuilder.newBuilder()
            .maximumSize(cacheMaxSize)
            .expireAfterWrite(cacheTtlSec, TimeUnit.SECONDS)
            .build();

    topologyName = context.getTopologyName();



    int port = TypeUtils.getInteger(conf.getOrDefault(KEY_PORT, 0));
    if (port == 0) {
      if (!Strings.isNullOrEmpty(portFile)) {
        try {
          port = TypeUtils.getInteger(Files.lines(Paths.get(portFile)).findFirst()
                  .get().trim());
        } catch (IOException | SecurityException | IllegalArgumentException e) {
          throw new IllegalArgumentException("Could not parse " + KEY_PORT_FILE + " " + portFile
                  + " Make sure the file is readable,"
                  + " only contains the port on which the service should run"
                  + " and is UTF8 encoded", e);
        }
      } else {
        throw new IllegalArgumentException("Neither 'port' nor 'port_file' "
            + "were specified in config for metrics sink " + context.getSinkId());
      }
    }
    startHttpServer(path, port);
  }

  /**
   * Start a http server on supplied port that will serve the metrics, as json,
   * on the specified path.
   *
   * @param path
   * @param port
   */
  protected void startHttpServer(String path, int port) {
    try {
      httpServer = HttpServer.create(new InetSocketAddress(port), 0);
      httpServer.createContext(path, new HttpHandler() {
        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
          metricsCache.cleanUp();
          byte[] response = MAPPER.writeValueAsString(metricsCache.asMap())
                  .getBytes();
          httpExchange.sendResponseHeaders(HTTP_STATUS_OK, response.length);
          OutputStream os = httpExchange.getResponseBody();
          os.write(response);
          os.close();
          LOG.log(Level.INFO, "Received metrics request.");
        }
      });
      httpServer.start();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create Http server on port " + port, e);
    }
  }

  /**
   * Helper to prefix metric names, convert metric value to double and return as map
   *
   * @param prefix
   * @param metrics
   * @return Map of metric name to metric value
   */
  private Map<String, Double> processMetrics(String prefix, Iterable<MetricsInfo> metrics) {
    Map<String, Double> map = new HashMap<>();
    for (MetricsInfo r : metrics) {
      try {
        map.put(prefix + r.getName(), Double.valueOf(r.getValue()));
      } catch (NumberFormatException ne) {
        LOG.log(Level.SEVERE, "Could not parse metric, Name: "
                + r.getName() + " Value: " + r.getValue(), ne);
        continue;
      }
    }
    return map;
  }

  @Override
  public void processRecord(MetricsRecord record) {
    String[] sources = record.getSource().split("/");
    String source;

    if (sources.length > 2) {
      source = includeTopologyName
          ? String.format("%s/%s/%s", topologyName, sources[1], sources[2])
          : String.format("/%s/%s", sources[1], sources[2]);
    } else {
      source = includeTopologyName
          ? String.format("%s/%s", topologyName, record.getSource())
          : String.format("/%s", record.getSource());
    }

    if (isFlatMetrics) {
      metricsCache.putAll(processMetrics(source + "/", record.getMetrics()));
    } else {
      Map<String, Double> sourceMap = processMetrics("", record.getMetrics());
      metricsCache.put(source, sourceMap);
    }
  }

  @Override
  public void flush() { }

  @Override
  public void close() {
    httpServer.stop(0);
  }
}
