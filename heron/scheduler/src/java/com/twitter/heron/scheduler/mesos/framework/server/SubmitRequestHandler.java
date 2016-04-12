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
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import com.twitter.heron.scheduler.mesos.framework.jobs.BaseJob;
import com.twitter.heron.scheduler.mesos.framework.jobs.JobScheduler;
import com.twitter.heron.scheduler.util.NetworkUtility;

public class SubmitRequestHandler implements HttpHandler {
  private static final Logger LOG = Logger.getLogger(SubmitRequestHandler.class.getName());

  private final JobScheduler jobScheduler;

  public SubmitRequestHandler(JobScheduler jobScheduler) {
    this.jobScheduler = jobScheduler;
  }

  @Override
  public void handle(HttpExchange httpExchange) throws IOException {
    LOG.info("Received a submit request from client");
    byte[] requestBody = NetworkUtility.readHttpRequestBody(httpExchange);
    LOG.info("Got request body!");

    // Parse the String into JSON
    String jobDefinitionInJSON = new String(requestBody);
    BaseJob baseJob;
    try {
      baseJob = BaseJob.getJobFromJSONString(jobDefinitionInJSON);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Unable to get baseJob. ", e);
      NetworkUtility.sendHttpResponse(false, httpExchange, new byte[0]);
      return;
    }

    boolean isSuccess = false;

    try {
      isSuccess = handleRequest(baseJob);
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Failed to submit job: " + baseJob.name, e);
    }

    if (isSuccess) {

      LOG.info("Submit done! Send OK response!");
    } else {
      LOG.info("Failed to submit topology! Send NOT_AVAILABLE response!");
    }

    // Send back the response
    NetworkUtility.sendHttpResponse(isSuccess, httpExchange, new byte[0]);
  }

  // Pre-condition:
  // 1. There is no old job with the same name. The condition is sane
  // 2. All info in JobDefinition is valid and sane
  protected boolean handleRequest(BaseJob newJob) {
    LOG.info("Received request for job: " + newJob.toString());

    if (jobScheduler.registerJob(newJob)) {

      LOG.info("Added job:: " + newJob.name);
      return true;
    } else {
      LOG.info("Failed to add job: " + newJob.name);
      return false;
    }
  }
}
