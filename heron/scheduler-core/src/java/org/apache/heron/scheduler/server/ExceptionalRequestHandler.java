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
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.scheduler.utils.Runtime;
import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.scheduler.IScheduler;
import org.apache.heron.spi.utils.NetworkUtils;

/**
 * {@code HttpHandler} decorator that returns an OK response unless an exception is caught, in
 * which case a NOTOK response is returned. If the delegate throws a
 * {@code TerminateSchedulerException}, the scheduler will be terminated after the response is
 * handled.
 */
class ExceptionalRequestHandler implements HttpHandler {
  private static final Logger LOG = Logger.getLogger(ExceptionalRequestHandler.class.getName());
  private HttpHandler delegate;
  private Config runtime;
  private IScheduler scheduler;

  ExceptionalRequestHandler(HttpHandler delegate, Config runtime, IScheduler scheduler) {
    this.delegate = delegate;
    this.runtime = runtime;
    this.scheduler = scheduler;
  }

  @SuppressWarnings("IllegalCatch")
  @Override
  public void handle(HttpExchange exchange) throws IOException {

    try {
      delegate.handle(exchange);
      sendResponse(exchange, true);
    } catch (TerminateSchedulerException e) {
      sendResponse(exchange, true);

      // tell the scheduler to shutdown
      LOG.info("Request handler issuing a terminate request to scheduler");
      try {
        scheduler.close();
      } finally {
        Runtime.schedulerShutdown(runtime).terminate();
      }
    } catch (Exception e) {
      handleFailure(exchange, e);
    }
  }

  private static void handleFailure(HttpExchange exchange, Exception e) {
    LOG.log(Level.SEVERE,
        String.format("Failed to handle %s request", exchange.getRequestURI()), e);
    sendResponse(exchange, false);
  }

  private static void sendResponse(HttpExchange exchange, boolean success) {
    Scheduler.SchedulerResponse response = SchedulerUtils.constructSchedulerResponse(success);
    NetworkUtils.sendHttpResponse(exchange, response.toByteArray());
  }
}
