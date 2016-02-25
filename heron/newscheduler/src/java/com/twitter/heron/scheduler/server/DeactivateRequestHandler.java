package com.twitter.heron.scheduler.server;

import java.io.IOException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import com.twitter.heron.proto.scheduler.Scheduler;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.HttpUtils;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.NetworkUtils;


public class DeactivateRequestHandler implements HttpHandler {

  private IScheduler scheduler;

  public DeactivateRequestHandler(Config runtime, IScheduler scheduler) {
    this.scheduler = scheduler;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {

    // read the http request payload
    byte[] requestBody = HttpUtils.readHttpRequestBody(exchange);

    // prepare the deactivate request
    Scheduler.DeactivateTopologyRequest deactivateTopologyRequest =
        Scheduler.DeactivateTopologyRequest.newBuilder()
            .mergeFrom(requestBody)
            .build();

    // deactivate the topology
    boolean isDeactivatedSuccessfully = scheduler.onDeactivate(deactivateTopologyRequest);

    // prepare the response
    Scheduler.DeactivateTopologyResponse response =
        Scheduler.DeactivateTopologyResponse.newBuilder()
            .setStatus(NetworkUtils.getHeronStatus(isDeactivatedSuccessfully))
            .build();

    // Send the response back
    HttpUtils.sendHttpResponse(exchange, response.toByteArray());
  }
}
