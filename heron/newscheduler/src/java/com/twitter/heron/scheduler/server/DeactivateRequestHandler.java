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
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.HttpUtils;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.NetworkUtils;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.TMasterUtils;


public class DeactivateRequestHandler implements HttpHandler {

    private IScheduler scheduler;
    private Config runtime;

    public DeactivateRequestHandler(Config runtime, IScheduler scheduler) {
        this.scheduler = scheduler;
        this.runtime = runtime;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {

        // read the http request payload
        byte[] requestBody = HttpUtils.readHttpRequestBody(exchange);

        // prepare the deactivate request
        Scheduler.DeactivateTopologyRequest deactivateTopologyRequest =
                Scheduler.DeactivateTopologyRequest.newBuilder()
                        .mergeFrom(requestBody)
                        .build();

        // deactivate the topology
        boolean isDeactivatedSuccessfully =
                scheduler.onDeactivate(deactivateTopologyRequest) &&
                        TMasterUtils.sendToTMaster("deactivate",
                                Runtime.topologyName(runtime),
                                Runtime.schedulerStateManagerAdaptor(runtime));

        // prepare the response
        Scheduler.DeactivateTopologyResponse response =
                Scheduler.DeactivateTopologyResponse.newBuilder()
                        .setStatus(NetworkUtils.getHeronStatus(isDeactivatedSuccessfully))
                        .build();

        // Send the response back
        HttpUtils.sendHttpResponse(exchange, response.toByteArray());
    }
}
