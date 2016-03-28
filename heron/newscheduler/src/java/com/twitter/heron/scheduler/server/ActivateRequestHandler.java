package com.twitter.heron.scheduler.server;

import java.io.IOException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import com.twitter.heron.proto.scheduler.Scheduler;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.HttpUtils;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.NetworkUtils;

public class ActivateRequestHandler implements HttpHandler {

  private IScheduler scheduler;

  public ActivateRequestHandler(Config runtime, IScheduler scheduler) {
    this.scheduler = scheduler;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    
    // read the http request payload
    byte[] requestBody = HttpUtils.readHttpRequestBody(exchange);

    // prepare the activate request
    Scheduler.ActivateTopologyRequest activateTopologyRequest = 
        Scheduler.ActivateTopologyRequest.newBuilder()
            .mergeFrom(requestBody)
            .build();

    // activate the topology
    boolean isActivatedSuccessfully = scheduler.onActivate(activateTopologyRequest);

    // prepare the response
    Scheduler.ActivateTopologyResponse response = 
        Scheduler.ActivateTopologyResponse.newBuilder()
            .setStatus(NetworkUtils.getHeronStatus(isActivatedSuccessfully))
            .build();

    // send the response back
    HttpUtils.sendHttpResponse(exchange, response.toByteArray());
  }
}
