package com.twitter.heron.scheduler.service.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.HttpServer;

import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.scheduler.context.LaunchContext;
import com.twitter.heron.scheduler.util.NetworkUtility;

public class SchedulerServer {
  public static final String KILL_REQUEST_CONTEXT = "/kill";

  public static final String ACTIVATE_REQUEST_CONTEXT = "/activate";

  public static final String DEACTIVATE_REQUEST_CONTEXT = "/deactivate";

  public static final String RESTART_REQUEST_CONTEXT = "/restart";

  private static final int SERVER_BACK_LOG = 0;

  private final HttpServer schedulerServer;
  private final LaunchContext context;

  public SchedulerServer(IScheduler scheduler, LaunchContext context, int port, boolean initialize)
      throws IOException {
    this.schedulerServer = getSchedulerServer(port);
    this.context = context;

    if (initialize) {
      initializeContext(scheduler);
    }
  }

  protected void initializeContext(IScheduler scheduler) throws IOException {
    this.schedulerServer.createContext(KILL_REQUEST_CONTEXT,
        new KillRequestHandler(scheduler, context));

    this.schedulerServer.createContext(ACTIVATE_REQUEST_CONTEXT,
        new ActivateRequestHandler(scheduler, context));

    this.schedulerServer.createContext(DEACTIVATE_REQUEST_CONTEXT,
        new DeactivateRequestHandler(scheduler, context));

    this.schedulerServer.createContext(RESTART_REQUEST_CONTEXT,
        new RestartRequestHandler(scheduler, context));
  }

  public void start() {
    schedulerServer.start();
  }

  public void stop() {
    schedulerServer.stop(0);
  }

  public String getHost() {
    return NetworkUtility.getHostName();
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
