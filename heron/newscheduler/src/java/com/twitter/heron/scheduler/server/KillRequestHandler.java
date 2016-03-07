package com.twitter.heron.scheduler.server;

import java.io.IOException;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import com.twitter.heron.proto.scheduler.Scheduler;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.HttpUtils;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.NetworkUtils;

public class KillRequestHandler implements HttpHandler {
  private static final Logger LOG = Logger.getLogger(KillRequestHandler.class.getName());

  private IScheduler scheduler;
  private Config runtime;

  public KillRequestHandler(Config runtime, IScheduler scheduler) {
    this.scheduler = scheduler;
    this.runtime = runtime;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {

    // read the http request payload
    byte[] requestBody = HttpUtils.readHttpRequestBody(exchange);

    // prepare the kill topology request
    Scheduler.KillTopologyRequest killTopologyRequest =
        Scheduler.KillTopologyRequest.newBuilder()
            .mergeFrom(requestBody)
            .build();

    // kill the topology
    boolean isKillSuccessfully = scheduler.onKill(killTopologyRequest);

    // prepare the response
    Scheduler.KillTopologyResponse response =
        Scheduler.KillTopologyResponse.newBuilder()
            .setStatus(NetworkUtils.getHeronStatus(isKillSuccessfully))
            .build();

    // Send the response back
    HttpUtils.sendHttpResponse(exchange, response.toByteArray());

    // tell the scheduler to shutdown
    LOG.info("Kill request handler issuing a terminate request to scheduler");
    Runtime.schedulerShutdown(runtime).terminate();
  }
}
