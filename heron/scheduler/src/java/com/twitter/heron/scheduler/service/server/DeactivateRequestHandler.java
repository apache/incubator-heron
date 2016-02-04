package com.twitter.heron.scheduler.service.server;

import java.io.IOException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.scheduler.context.LaunchContext;
import com.twitter.heron.scheduler.util.NetworkUtility;

public class DeactivateRequestHandler implements HttpHandler {
  IScheduler scheduler;

  public DeactivateRequestHandler(IScheduler scheduler, LaunchContext context) {
    this.scheduler = scheduler;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    byte[] requestBody = NetworkUtility.readHttpRequestBody(exchange);

    Scheduler.DeactivateTopologyRequest deactivateTopologyRequest =
        Scheduler.DeactivateTopologyRequest.newBuilder().
            mergeFrom(requestBody).
            build();

    boolean isDeactivatedSuccessfully = scheduler.onDeactivate(deactivateTopologyRequest);

    Scheduler.DeactivateTopologyResponse response =
        Scheduler.DeactivateTopologyResponse.newBuilder().
            setStatus(NetworkUtility.getHeronStatus(isDeactivatedSuccessfully)).
            build();

    // Send it back!
    NetworkUtility.sendHttpResponse(exchange, response.toByteArray());
  }
}
