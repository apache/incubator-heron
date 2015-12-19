package com.twitter.heron.scheduler.service.server;

import java.io.IOException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.api.IScheduler;
import com.twitter.heron.scheduler.api.context.LaunchContext;
import com.twitter.heron.scheduler.util.NetworkUtility;

public class ActivateRequestHandler implements HttpHandler {
  IScheduler scheduler;

  public ActivateRequestHandler(IScheduler scheduler, LaunchContext context) {
    this.scheduler = scheduler;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    byte[] requestBody = NetworkUtility.readHttpRequestBody(exchange);

    Scheduler.ActivateTopologyRequest activateTopologyRequest =
        Scheduler.ActivateTopologyRequest.newBuilder().
            mergeFrom(requestBody).
            build();

    boolean isActivatedSuccessfully = scheduler.onActivate(activateTopologyRequest);

    Scheduler.ActivateTopologyResponse response =
        Scheduler.ActivateTopologyResponse.newBuilder().
            setStatus(NetworkUtility.getHeronStatus(isActivatedSuccessfully)).
            build();

    // Send it back!
    NetworkUtility.sendHttpResponse(exchange, response.toByteArray());
  }
}
