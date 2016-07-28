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

package com.twitter.heron.scheduler.server;

import java.io.IOException;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.spi.common.SpiCommonConfig;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.NetworkUtils;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.SchedulerUtils;

public class KillRequestHandler implements HttpHandler {
  private static final Logger LOG = Logger.getLogger(KillRequestHandler.class.getName());

  private IScheduler scheduler;
  private SpiCommonConfig runtime;

  public KillRequestHandler(SpiCommonConfig runtime, IScheduler scheduler) {
    this.scheduler = scheduler;
    this.runtime = runtime;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {

    // read the http request payload
    byte[] requestBody = NetworkUtils.readHttpRequestBody(exchange);

    // prepare the kill topology request
    Scheduler.KillTopologyRequest killTopologyRequest =
        Scheduler.KillTopologyRequest.newBuilder()
            .mergeFrom(requestBody)
            .build();

    // kill the topology
    boolean isKillSuccessfully = scheduler.onKill(killTopologyRequest);

    // prepare the response
    Scheduler.SchedulerResponse response =
        SchedulerUtils.constructSchedulerResponse(isKillSuccessfully);

    // Send the response back
    NetworkUtils.sendHttpResponse(exchange, response.toByteArray());

    // tell the scheduler to shutdown
    LOG.info("Kill request handler issuing a terminate request to scheduler");
    Runtime.schedulerShutdown(runtime).terminate();
  }
}
