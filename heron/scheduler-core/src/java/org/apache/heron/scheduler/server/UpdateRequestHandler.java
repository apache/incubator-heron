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
