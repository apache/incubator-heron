// Copyright 2017 Twitter. All rights reserved.
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

package com.twitter.heron.scheduler.utils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.twitter.heron.scheduler.TopologyRuntimeManagementException;
import com.twitter.heron.spi.utils.NetworkUtils;

/**
 *  Utility functions for talking to a JSON-based scheduler API
 */
public final class SchedulerJsonAPIUtils {

  private static final Logger LOG = Logger.getLogger(SchedulerJsonAPIUtils.class.getName());

  private SchedulerJsonAPIUtils() {

  }

  private static HttpURLConnection getUrlConnection(String uri) {
    HttpURLConnection conn = NetworkUtils.getHttpConnection(uri);
    if (conn == null) {
      throw new TopologyRuntimeManagementException("Failed to initialize connection to " + uri);
    }
    return conn;
  }

  /**
   * Make a GET request to a JSON-based API endpoint
   *
   * @param uri, URI to make an HTTP GET request to
   * @param expectedResponseCode, the response code you expect to get from this endpoint
   * @return JSON result, the result in the form of a parsed JsonNode
   */
  public static JsonNode schedulerGetRequest(String uri, Integer expectedResponseCode) {
    HttpURLConnection conn = getUrlConnection(uri);
    byte[] responseData;
    try {
      if (!NetworkUtils.sendHttpGetRequest(conn)) {
        throw new TopologyRuntimeManagementException("Failed to send delete request to " + uri);
      }

      // Check the response code
      if (!NetworkUtils.checkHttpResponseCode(conn, expectedResponseCode)) {
        throw new TopologyRuntimeManagementException("Unexpected response from connection. Expected"
            + expectedResponseCode + " but received " + NetworkUtils.getHttpResponseCode(conn));
      }

      responseData = NetworkUtils.readHttpResponse(conn);

    } finally {
      conn.disconnect();
    }

    // parse of the json
    JsonNode podDefinition;

    try {
      // read the json data
      ObjectMapper mapper = new ObjectMapper();
      podDefinition = mapper.readTree(responseData);
    } catch (IOException ioe) {
      throw new TopologyRuntimeManagementException("Failed to parse JSON response from "
          + "Scheduler API");
    }

    return podDefinition;
  }

  /**
   * Make a DELETE request to a scheduler
   *
   * @param uri, URI to make an HTTP DELETE request to
   * @param expectedResponseCode, the response code you expect to get from this endpoint
   */
  public static void schedulerDeleteRequest(String uri, Integer expectedResponseCode) {
    HttpURLConnection conn = getUrlConnection(uri);
    try {
      if (!NetworkUtils.sendHttpDeleteRequest(conn)) {
        throw new TopologyRuntimeManagementException("Failed to send delete request to " + uri);
      }

      // Check the response code
      if (!NetworkUtils.checkHttpResponseCode(conn, expectedResponseCode)) {
        throw new TopologyRuntimeManagementException("Unexpected response from connection. Expected"
            + expectedResponseCode + " but received "
            + NetworkUtils.getHttpResponseCode(conn));
      }
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Make a POST request to a scheduler
   *
   * @param uri, URI to make an HTTP POST request to
   * @param jsonBody, String form of the jsonBody
   * @param expectedResponseCode, the response code you expect to get from this endpoint
   */
  public static void schedulerPostRequest(String uri, String jsonBody, Integer expectedResponseCode) {
    HttpURLConnection conn = getUrlConnection(uri);

    try {
      // send post request with json body for the topology
      if (!NetworkUtils.sendHttpPostRequest(conn,
          NetworkUtils.JSON_TYPE,
          jsonBody.getBytes())) {
        throw new TopologyRuntimeManagementException("Failed to send POST to " + uri);
      }

      // check the response
      if(!NetworkUtils.checkHttpResponseCode(conn, expectedResponseCode)) {
        byte[] bytes = NetworkUtils.readHttpResponse(conn);
        LOG.log(Level.SEVERE, "Failed to send POST request to scheduler");
        LOG.log(Level.SEVERE, Arrays.toString(bytes));
        throw new TopologyRuntimeManagementException("Unexpected response from connection. Expected"
            + expectedResponseCode + " but received "
            + NetworkUtils.getHttpResponseCode(conn));
      }
    } finally {
      conn.disconnect();
    }
  }

}
