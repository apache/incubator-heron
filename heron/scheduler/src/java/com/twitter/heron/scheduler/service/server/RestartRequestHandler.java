package com.twitter.heron.scheduler.service.server;

import java.io.IOException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.scheduler.context.LaunchContext;
import com.twitter.heron.scheduler.util.NetworkUtility;

public class RestartRequestHandler implements HttpHandler {
  IScheduler scheduler;

  public RestartRequestHandler(IScheduler scheduler, LaunchContext context) {
    this.scheduler = scheduler;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    byte[] requestBody = NetworkUtility.readHttpRequestBody(exchange);

    Scheduler.RestartTopologyRequest restartTopologyRequest =
        Scheduler.RestartTopologyRequest.newBuilder().
            mergeFrom(requestBody).
            build();

    boolean isRestartSuccessfully = scheduler.onRestart(restartTopologyRequest);

    Scheduler.RestartTopologyResponse response =
        Scheduler.RestartTopologyResponse.newBuilder().
            setStatus(NetworkUtility.getHeronStatus(isRestartSuccessfully)).
            build();

    // Send it back!
    NetworkUtility.sendHttpResponse(exchange, response.toByteArray());
  }
}
