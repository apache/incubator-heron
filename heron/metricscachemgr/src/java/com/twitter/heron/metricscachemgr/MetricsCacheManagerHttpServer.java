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
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.GeneratedMessageV3;
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
  private static final String PATH_STATS = "/stats";
  private static final String PATH_EXCEPTIONS = "/exceptions";
  private static final String PATH_EXCEPTIONSUMMARY = "/exceptionsummary";

  private static final Logger LOG = Logger.getLogger(MetricsCacheManagerHttpServer.class.getName());
  // http server
  private final HttpServer server;
  // reference to MetricsCache object
  private final MetricsCache metricsCache;

  public MetricsCacheManagerHttpServer(MetricsCache cache, int port) throws IOException {
    metricsCache = cache;

    server = HttpServer.create(new InetSocketAddress(port), 0);

    server.createContext(PATH_STATS, new HandleStatsRequest());
    server.createContext(PATH_EXCEPTIONS, new HandleExceptionRequest());
    server.createContext(PATH_EXCEPTIONSUMMARY, new HandleExceptionSummaryRequest());
  }

  /**
   * manual test for local mode topology, for debug
   * How to run:
   * in the [source root directory], run bazel test,
   * bazel run heron/metricscachemgr/src/java:metricscache-queryclient-unshaded -- \
   * &lt;host:port&gt; &lt;component_name&gt; &lt;metrics_name&gt;
   * Example:
   * 1. run the example topology,
   * ~/bin/heron submit local ~/.heron/examples/heron-examples.jar \
   * com.twitter.heron.examples.ExclamationTopology ExclamationTopology \
   * --deploy-deactivated --verbose
   * 2. in the [source root directory],
   * bazel run heron/metricscachemgr/src/java:metricscache-queryclient-unshaded -- \
   * 127.0.0.1:23345 exclaim1 \
   * __emit-count __execute-count __fail-count __ack-count __complete-latency __execute-latency \
   * __process-latency __jvm-uptime-secs __jvm-process-cpu-load __jvm-memory-used-mb \
   * __jvm-memory-mb-total __jvm-gc-collection-time-ms __server/__time_spent_back_pressure_initiated \
   * __time_spent_back_pressure_by_compid
   */
  public static void main(String[] args)
      throws ExecutionException, InterruptedException, IOException {
    if (args.length < 3) {
      System.out.println(
          "Usage: java MetricsQuery <host:port> <component_name> <metrics_name>");
    }

    // construct metric cache stat url
    String url = "http://" + args[0] + MetricsCacheManagerHttpServer.PATH_STATS;
    System.out.println("endpoint: " + url + "; component: " + args[1]);

    // construct query payload
    byte[] requestData = TopologyMaster.MetricRequest.newBuilder()
        .setComponentName(args[1])
        .setMinutely(true)
        .setInterval(-1)
        .addAllMetric(Arrays.asList(Arrays.copyOfRange(args, 2, args.length)))
        .build().toByteArray();

    // http communication
    HttpURLConnection con = NetworkUtils.getHttpConnection(url);
    NetworkUtils.sendHttpPostRequest(con, "X", requestData);
    byte[] responseData = NetworkUtils.readHttpResponse(con);

    // parse response data
    TopologyMaster.MetricResponse response = TopologyMaster.MetricResponse.parseFrom(responseData);

    System.out.println(response.toString());
  }

  public void start() {
    server.start();
  }

  public void stop() {
    server.stop(0);
  }

  // T - request, U - response
  abstract class RequestHandler<T extends GeneratedMessageV3, U extends GeneratedMessageV3>
      implements HttpHandler {
    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
      // get the entire stuff
      byte[] payload = NetworkUtils.readHttpRequestBody(httpExchange);
      T req;
      try {
        req = parseRequest(payload);
      } catch (InvalidProtocolBufferException e) {
        LOG.log(Level.SEVERE,
            "Unable to decipher data specified in Request: " + httpExchange, e);
        httpExchange.sendResponseHeaders(400, -1); // throw exception
        return;
      }
      U res = generateResponse(req, metricsCache);
      NetworkUtils.sendHttpResponse(httpExchange, res.toByteArray());
      httpExchange.close();
    }

    abstract T parseRequest(byte[] requestBytes) throws InvalidProtocolBufferException;

    abstract U generateResponse(T request, MetricsCache metricsCache1);
  }

  // compatible with tmaster stat interface: http+protobuf
  class HandleStatsRequest
      extends RequestHandler<TopologyMaster.MetricRequest, TopologyMaster.MetricResponse> {
    @Override
    public TopologyMaster.MetricRequest parseRequest(byte[] requestBytes)
        throws InvalidProtocolBufferException {
      return TopologyMaster.MetricRequest.parseFrom(requestBytes);
    }

    @Override
    public TopologyMaster.MetricResponse generateResponse(
        TopologyMaster.MetricRequest request, MetricsCache metricsCache1) {
      return metricsCache1.getMetrics(request);
    }
  }

  // compatible with tmaster exceptions interface: http+protobuf
  public class HandleExceptionRequest extends
      RequestHandler<TopologyMaster.ExceptionLogRequest, TopologyMaster.ExceptionLogResponse> {
    @Override
    public TopologyMaster.ExceptionLogRequest parseRequest(byte[] requestBytes)
        throws InvalidProtocolBufferException {
      return TopologyMaster.ExceptionLogRequest.parseFrom(requestBytes);
    }

    @Override
    public TopologyMaster.ExceptionLogResponse generateResponse(
        TopologyMaster.ExceptionLogRequest request, MetricsCache metricsCache1) {
      return metricsCache1.getExceptions(request);
    }
  }

  // compatible with tmaster exceptionsummary interface: http+protobuf
  public class HandleExceptionSummaryRequest extends
      RequestHandler<TopologyMaster.ExceptionLogRequest, TopologyMaster.ExceptionLogResponse> {
    @Override
    public TopologyMaster.ExceptionLogRequest parseRequest(byte[] requestBytes)
        throws InvalidProtocolBufferException {
      return TopologyMaster.ExceptionLogRequest.parseFrom(requestBytes);
    }

    @Override
    public TopologyMaster.ExceptionLogResponse generateResponse(
        TopologyMaster.ExceptionLogRequest request, MetricsCache metricsCache1) {
      return metricsCache1.getExceptionsSummary(request);
    }
  }

  // compatible with tracker: http+json
  // TODO(huijun) add compatible query interface for tracker
  public class MetricsHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
      httpExchange.close();
    }
  }
}
