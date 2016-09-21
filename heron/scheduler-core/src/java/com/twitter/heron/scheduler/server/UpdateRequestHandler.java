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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.NetworkUtils;

class UpdateRequestHandler implements HttpHandler {
  private IScheduler scheduler;

  UpdateRequestHandler(IScheduler scheduler) {
    this.scheduler = scheduler;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {

    // read the http request payload
    byte[] requestBody = NetworkUtils.readHttpRequestBody(exchange);

    // prepare the request
    Scheduler.UpdateTopologyRequest updateTopologyRequest =
        Scheduler.UpdateTopologyRequest.newBuilder()
            .mergeFrom(requestBody)
            .build();

    if (!scheduler.onUpdate(updateTopologyRequest)) {
      throw new RuntimeException("Failed to process updateTopologyRequest");
    }
  }
}
