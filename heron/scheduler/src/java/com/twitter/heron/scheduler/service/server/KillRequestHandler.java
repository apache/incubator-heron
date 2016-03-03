package com.twitter.heron.scheduler.service.server;

import java.io.IOException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.scheduler.context.LaunchContext;
import com.twitter.heron.scheduler.util.NetworkUtility;

public class KillRequestHandler implements HttpHandler {
  IScheduler scheduler;
  LaunchContext context;

  public KillRequestHandler(IScheduler scheduler, LaunchContext context) {
    this.scheduler = scheduler;
    this.context = context;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    byte[] requestBody = NetworkUtility.readHttpRequestBody(exchange);

    Scheduler.KillTopologyRequest killTopologyRequest =
        Scheduler.KillTopologyRequest.newBuilder().
            mergeFrom(requestBody).
            build();

    boolean isKillSuccessfully = scheduler.onKill(killTopologyRequest);

    Scheduler.KillTopologyResponse response =
        Scheduler.KillTopologyResponse.newBuilder().
            setStatus(NetworkUtility.getHeronStatus(isKillSuccessfully)).
            build();

    // Send it back!
    NetworkUtility.sendHttpResponse(exchange, response.toByteArray());

    context.close();
    System.exit(0);
  }
}
