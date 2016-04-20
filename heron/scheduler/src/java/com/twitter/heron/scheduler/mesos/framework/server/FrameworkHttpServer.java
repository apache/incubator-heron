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

package com.twitter.heron.scheduler.mesos.framework.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpServer;

import com.twitter.heron.scheduler.mesos.framework.jobs.JobScheduler;

public class FrameworkHttpServer {
  public static final String KILL_REQUEST_CONTEXT = "/kill";
  public static final String SUBMIT_REQUEST_CONTEXT = "/submit";
  private static final Logger LOG = Logger.getLogger(FrameworkHttpServer.class.getName());
  private static final int SERVER_BACK_LOG = 0;

  private final HttpServer schedulerServer;

  public FrameworkHttpServer(JobScheduler jobScheduler, int port, boolean initialize)
      throws IOException {
    this.schedulerServer = getSchedulerServer(port);

    if (initialize) {
      initializeContext(jobScheduler);
    }
  }

  protected void initializeContext(JobScheduler jobScheduler) throws IOException {
    this.schedulerServer.createContext(KILL_REQUEST_CONTEXT,
        new KillRequestHandler(jobScheduler));

    this.schedulerServer.createContext(SUBMIT_REQUEST_CONTEXT,
        new SubmitRequestHandler(jobScheduler));
  }

  public void start() {
    schedulerServer.start();
    LOG.info("Starting listen on port:" + getPort());
  }

  public void stop() {
    schedulerServer.stop(0);
  }

  public String getHost() {
    String hostName;
    try {
      hostName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "Unable to get local host name", e);
      hostName = "localhost";
    }

    return hostName;
  }

  public int getPort() {
    return schedulerServer.getAddress().getPort();
  }

  protected HttpServer getSchedulerServer(int port) throws IOException {
    HttpServer schedulerServer = HttpServer.create(new InetSocketAddress(port), SERVER_BACK_LOG);
    schedulerServer.setExecutor(Executors.newSingleThreadExecutor());

    return schedulerServer;
  }
}
