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
  private static final Logger LOG = Logger.getLogger(FrameworkHttpServer.class.getName());

  public static final String KILL_REQUEST_CONTEXT = "/kill";

  public static final String SUBMIT_REQUEST_CONTEXT = "/submit";

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
