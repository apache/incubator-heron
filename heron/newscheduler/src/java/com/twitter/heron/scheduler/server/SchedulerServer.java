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
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpServer;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.HttpUtils;
import com.twitter.heron.spi.scheduler.IScheduler;

public class SchedulerServer {

  // initialize the various URL end points
  public static final String KILL_REQUEST_CONTEXT = "/kill";
  public static final String ACTIVATE_REQUEST_CONTEXT = "/activate";
  public static final String DEACTIVATE_REQUEST_CONTEXT = "/deactivate";
  public static final String RESTART_REQUEST_CONTEXT = "/restart";

  private static final int SERVER_BACK_LOG = 0;

  private final HttpServer schedulerServer;
  private final Config runtime;

  public SchedulerServer(Config runtime, IScheduler scheduler, int port)
      throws IOException {

    this.runtime = runtime;
    this.schedulerServer = createServer(port);

    // associate handlers with the URL service end points
    this.schedulerServer.createContext(KILL_REQUEST_CONTEXT,
        new KillRequestHandler(runtime, scheduler));

    this.schedulerServer.createContext(ACTIVATE_REQUEST_CONTEXT,
        new ActivateRequestHandler(runtime, scheduler));

    this.schedulerServer.createContext(DEACTIVATE_REQUEST_CONTEXT,
        new DeactivateRequestHandler(runtime, scheduler));

    this.schedulerServer.createContext(RESTART_REQUEST_CONTEXT,
        new RestartRequestHandler(runtime, scheduler));
  }

  public void start() {
    schedulerServer.start();
  }

  public void stop() {
    schedulerServer.stop(0);
  }

  public String getHost() {
    return HttpUtils.getHostName();
  }

  public int getPort() {
    return schedulerServer.getAddress().getPort();
  }

  protected HttpServer createServer(int port) throws IOException {
    HttpServer schedulerServer = HttpServer.create(new InetSocketAddress(port), SERVER_BACK_LOG);
    schedulerServer.setExecutor(Executors.newSingleThreadExecutor());
    return schedulerServer;
  }
}
