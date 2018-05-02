/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.scheduler.server;

import java.io.IOException;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.spi.scheduler.IScheduler;
import org.apache.heron.spi.utils.NetworkUtils;

public class KillRequestHandler implements HttpHandler {
  private IScheduler scheduler;

  KillRequestHandler(IScheduler scheduler) {
    this.scheduler = scheduler;
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

    if (!scheduler.onKill(killTopologyRequest)) {
      throw new RuntimeException("Failed to process killTopologyRequest");
    } else {
      throw new TerminateSchedulerException(
          "Kill request handled successfully - terminating scheduler");
    }
  }
}
