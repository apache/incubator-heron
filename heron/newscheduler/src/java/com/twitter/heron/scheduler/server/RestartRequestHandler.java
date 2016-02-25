package com.twitter.heron.scheduler.server;

import java.io.IOException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import com.twitter.heron.proto.scheduler.Scheduler;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.HttpUtils;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.NetworkUtils;

public class RestartRequestHandler implements HttpHandler {
  private IScheduler scheduler;

  public RestartRequestHandler(Config runtime, IScheduler scheduler) {
    this.scheduler = scheduler;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {

    // read the http request payload
    byte[] requestBody = HttpUtils.readHttpRequestBody(exchange);

    // prepare the request 
    Scheduler.RestartTopologyRequest restartTopologyRequest =
        Scheduler.RestartTopologyRequest.newBuilder()
            .mergeFrom(requestBody)
            .build();

    // restart the topology
    boolean isRestartSuccessfully = scheduler.onRestart(restartTopologyRequest);

    // prepare the response
    Scheduler.RestartTopologyResponse response =
        Scheduler.RestartTopologyResponse.newBuilder()
            .setStatus(NetworkUtils.getHeronStatus(isRestartSuccessfully))
            .build();

    // send the response back 
    HttpUtils.sendHttpResponse(exchange, response.toByteArray());
  }
}
