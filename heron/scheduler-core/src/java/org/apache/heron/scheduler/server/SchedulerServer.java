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
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpServer;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.scheduler.IScheduler;
import org.apache.heron.spi.utils.NetworkUtils;

public class SchedulerServer {

  // initialize the various URL end points
  private static final String KILL_REQUEST_CONTEXT = "/kill";
  private static final String RESTART_REQUEST_CONTEXT = "/restart";
  private static final String UPDATE_REQUEST_CONTEXT = "/update";

  private static final int SERVER_BACK_LOG = 0;

  private final HttpServer schedulerServer;
  private final IScheduler scheduler;
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  public SchedulerServer(Config runtime, IScheduler scheduler, int port)
      throws IOException {

    this.scheduler = scheduler;
    this.schedulerServer = createServer(port, executorService);

    // associate handlers with the URL service end points
    this.schedulerServer.createContext(KILL_REQUEST_CONTEXT,
        new ExceptionalRequestHandler(new KillRequestHandler(scheduler), runtime, scheduler));

    this.schedulerServer.createContext(RESTART_REQUEST_CONTEXT,
        new ExceptionalRequestHandler(new RestartRequestHandler(scheduler), runtime, scheduler));

    this.schedulerServer.createContext(UPDATE_REQUEST_CONTEXT,
        new ExceptionalRequestHandler(new UpdateRequestHandler(scheduler), runtime, scheduler));
  }

  public void start() {
    schedulerServer.start();
  }

  public void stop() {
    schedulerServer.stop(0);
    scheduler.close();

    // Stopping the server will not shut down the Executor
    // We have to shut it down explicitly
    executorService.shutdownNow();
  }

  public String getHost() {
    return NetworkUtils.getHostName();
  }

  public int getPort() {
    return schedulerServer.getAddress().getPort();
  }

  protected HttpServer createServer(int port, Executor executor) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress(port), SERVER_BACK_LOG);
    server.setExecutor(executor);
    return server;
  }
}
