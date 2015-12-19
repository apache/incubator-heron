package com.twitter.heron.scheduler.mesos.framework.server;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import com.twitter.heron.scheduler.mesos.framework.jobs.JobScheduler;
import com.twitter.heron.scheduler.util.NetworkUtility;

public class KillRequestHandler implements HttpHandler {
  private static final Logger LOG = Logger.getLogger(KillRequestHandler.class.getName());

  private final JobScheduler jobScheduler;

  public KillRequestHandler(JobScheduler jobScheduler) {
    this.jobScheduler = jobScheduler;
  }

  @Override
  public void handle(HttpExchange httpExchange) throws IOException {
    LOG.info("Received a kill request from client");
    byte[] requestBody = NetworkUtility.readHttpRequestBody(httpExchange);

    // Parse the String as jobName
    String jobName = new String(requestBody);
    LOG.info("To kill job: " + jobName);

    boolean isSuccess = false;

    try {
      isSuccess = handleRequest(jobName);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to kill job: " + jobName, e);
    }
    if (isSuccess) {
      LOG.info("Kill done! Send OK response!");
    } else {
      LOG.info("Failed to kill topology! Send NOT_AVAILABLE response!");
    }

    // Send back the response
    NetworkUtility.sendHttpResponse(isSuccess, httpExchange, new byte[0]);
  }

  protected boolean handleRequest(String jobName) {
    return jobScheduler.deregisterJob(jobName, false);
  }
}
