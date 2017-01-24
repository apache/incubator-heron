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

package com.twitter.heron.metricscachemgr;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import com.twitter.heron.metricscachemgr.metricscache.MetricsCache;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.utils.NetworkUtils;


/**
 * MetricsCacheMgr http server:
 * compatible with tmaster and tracker http interface for metrics
 * http path:
 * "/stats" metric query
 * "/exceptions" exception query
 * "/exceptionsummary" exception query, with aggregation
 * <p>
 * Differece from MetricsCacheManagerServer:
 * 1. MetricsCacheManagerServer accepts metric publishing message from sinks;
 * MetricsCacheManagerHttpServer responds to queries.
 * 2. MetricsCacheManagerServer is a HeronServer;
 * MetricsCacheManagerHttpServer is a http server
 */
public class MetricsCacheManagerHttpServer {
  // http path, compatible with tmaster stat interface
  public static final String PATH_STATS = "/stats";
  public static final String PATH_EXCEPTIONS = "/exceptions";
  public static final String PATH_EXCEPTIONSUMMARY = "/exceptionsummary";
  // logger
  private static final Logger LOG = Logger.getLogger(MetricsCacheManagerHttpServer.class.getName());
  // http server
  private HttpServer server = null;
  // reference to MetricsCache object
  private MetricsCache metricsCache = null;

  public MetricsCacheManagerHttpServer(MetricsCache cache, int port) throws IOException {
    metricsCache = cache;

    server = HttpServer.create(new InetSocketAddress(port), 0);

    server.createContext(PATH_STATS, new HandleStatsRequest());
    server.createContext(PATH_EXCEPTIONS, new HandleExceptionRequest());
    server.createContext(PATH_EXCEPTIONSUMMARY, new HandleExceptionSummaryRequest());
  }

  public void start() {
    server.start();
  }

  public void stop() {
    server.stop(0);
  }

  // compatible with tmaster stat interface: http+protobuf
  public class HandleStatsRequest implements HttpHandler {

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
      // get the entire stuff
      byte[] payload = NetworkUtils.readHttpRequestBody(httpExchange);
      TopologyMaster.MetricRequest req;
      try {
        req = TopologyMaster.MetricRequest.parseFrom(payload);
      } catch (InvalidProtocolBufferException e) {
        LOG.severe("Unable to decipher data specified in StatsRequest " + e.toString());
        httpExchange.sendResponseHeaders(400, -1); // throw exception
        return;
      }
      // query cache
      TopologyMaster.MetricResponse res = metricsCache.getMetrics(req);
      // response
      NetworkUtils.sendHttpResponse(httpExchange, res.toByteArray());
      // close
      httpExchange.close();
    }
  }

  // compatible with tmaster stat interface: http+protobuf
  public class HandleExceptionRequest implements HttpHandler {

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
      // get the entire stuff
      byte[] payload = NetworkUtils.readHttpRequestBody(httpExchange);
      TopologyMaster.ExceptionLogRequest req;
      try {
        req = TopologyMaster.ExceptionLogRequest.parseFrom(payload);
      } catch (InvalidProtocolBufferException e) {
        LOG.severe("Unable to decipher data specified in ExceptionRequest");
        httpExchange.sendResponseHeaders(400, -1); // throw exception
        return;
      }
      // query cache
      TopologyMaster.ExceptionLogResponse res = metricsCache.getExceptions(req);
      // response
      NetworkUtils.sendHttpResponse(httpExchange, res.toByteArray());
      // close
      httpExchange.close();
      LOG.info("Done with exception request ");
    }
  }

  // compatible with tmaster stat interface: http+protobuf
  public class HandleExceptionSummaryRequest implements HttpHandler {

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
      // get the entire stuff
      byte[] payload = NetworkUtils.readHttpRequestBody(httpExchange);
      TopologyMaster.ExceptionLogRequest req;
      try {
        req = TopologyMaster.ExceptionLogRequest.parseFrom(payload);
      } catch (InvalidProtocolBufferException e) {
        LOG.severe("Unable to decipher data specified in ExceptionRequest");
        httpExchange.sendResponseHeaders(400, -1); // throw exception
        return;
      }
      // query cache
      TopologyMaster.ExceptionLogResponse res = metricsCache.getExceptionsSummary(req);
      // response
      NetworkUtils.sendHttpResponse(httpExchange, res.toByteArray());
      // close
      httpExchange.close();
      LOG.info("Done with exception summary request ");
    }
  }

  // compatible with tracker: http+json
  public class MetricsHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
      httpExchange.close();
    }
  }
}
