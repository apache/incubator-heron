//  Copyright 2017 Twitter. All rights reserved.
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

package com.twitter.heron.metricsmgr.sink;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.sun.net.httpserver.HttpServer;

import com.twitter.heron.common.basics.TypeUtils;
import com.twitter.heron.spi.metricsmgr.sink.IMetricsSink;
import com.twitter.heron.spi.metricsmgr.sink.SinkContext;


/**
 * A metrics sink that publishes metrics on a http endpoint
 */
abstract class AbstractWebSink implements IMetricsSink {
  private static final Logger LOG = Logger.getLogger(AbstractWebSink.class.getName());

  private static final int HTTP_STATUS_OK = 200;

  // Metrics will be published on http://host:port/path, the port
  private static final String KEY_PORT = "port";

  // If you want to specify a file from which to read the port instead of
  // supplying it directly
  private static final String KEY_PORT_FILE = "port-file";

  // The path
  private static final String KEY_PATH = "path";

  // Maximum number of metrics getting served
  private static final String KEY_METRICS_CACHE_MAX_SIZE = "metrics-cache-max-size";
  private static final long DEFAULT_MAX_CACHE_SIZE = 1000000;

  // Time To Live before a metric gets evicted from the cache
  private static final String KEY_METRICS_CACHE_TTL_SEC = "metrics-cache-ttl-sec";
  private static final long DEFAULT_CACHE_TTL_SECONDS = 600;

  private HttpServer httpServer;
  private String topologyName;
  private long cacheMaxSize;
  private long cacheTtlSeconds;
  private final Ticker cacheTicker;

  AbstractWebSink() {
    this(Ticker.systemTicker());
  }

  @VisibleForTesting
  AbstractWebSink(Ticker cacheTicker) {
    this.cacheTicker = cacheTicker;
  }

  @Override
  public final void init(Map<String, Object> conf, SinkContext context) {
    String path = (String) conf.get(KEY_PATH);
    String portFile = (String) conf.get(KEY_PORT_FILE);

    cacheMaxSize = TypeUtils.getLong(conf.getOrDefault(KEY_METRICS_CACHE_MAX_SIZE,
        DEFAULT_MAX_CACHE_SIZE));

    cacheTtlSeconds = TypeUtils.getLong(conf.getOrDefault(KEY_METRICS_CACHE_TTL_SEC,
        DEFAULT_CACHE_TTL_SECONDS));

    topologyName = context.getTopologyName();

    // initialize child classes
    initialize(conf, context);

    int port = TypeUtils.getInteger(conf.getOrDefault(KEY_PORT, 0));
    if (port == 0) {
      if (!Strings.isNullOrEmpty(portFile)) {
        try (Stream<String> lines = Files.lines(Paths.get(portFile))) {
          port = TypeUtils.getInteger(lines.findFirst().get().trim());
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
      httpServer.createContext(path, httpExchange -> {
        byte[] response = generateResponse();
        httpExchange.sendResponseHeaders(HTTP_STATUS_OK, response.length);
        OutputStream os = httpExchange.getResponseBody();
        os.write(response);
        os.close();
        LOG.log(Level.INFO, "Received metrics request.");
      });
      httpServer.start();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create Http server on port " + port, e);
    }
  }

  // a convenience method for creating a metrics cache
  <K, V> Cache<K, V> createCache() {
    return CacheBuilder.newBuilder()
        .maximumSize(cacheMaxSize)
        .expireAfterWrite(cacheTtlSeconds, TimeUnit.SECONDS)
        .ticker(cacheTicker)
        .build();
  }

  String getTopologyName() {
    return topologyName;
  }

  abstract byte[] generateResponse() throws IOException;

  abstract void initialize(Map<String, Object> configuration, SinkContext context);

  @Override
  public void flush() { }

  @Override
  public void close() {
    httpServer.stop(0);
  }
}
