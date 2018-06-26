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

package org.apache.heron.scheduler.utils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.heron.spi.utils.NetworkUtils;

public class HttpJsonClient {
  private static final Logger LOG = Logger.getLogger(HttpJsonClient.class.getName());

  private String endpointURI;


  public HttpJsonClient(String endpointURI) {
    this.endpointURI = endpointURI;
  }

  private HttpURLConnection getUrlConnection() throws IOException {
    HttpURLConnection conn = NetworkUtils.getHttpConnection(endpointURI);
    if (conn == null) {
      throw new IOException("Failed to initialize connection to " + endpointURI);
    }
    return conn;
  }

  /**
   * Make a GET request to a JSON-based API endpoint
   *
   * @param expectedResponseCode the response code you expect to get from this endpoint
   * @return JSON result, the result in the form of a parsed JsonNode
   */
  public JsonNode get(Integer expectedResponseCode) throws IOException {
    HttpURLConnection conn = getUrlConnection();
    byte[] responseData;
    try {
      if (!NetworkUtils.sendHttpGetRequest(conn)) {
        throw new IOException("Failed to send get request to " + endpointURI);
      }

      // Check the response code
      if (!NetworkUtils.checkHttpResponseCode(conn, expectedResponseCode)) {
        throw new IOException("Unexpected response from connection. Expected "
            + expectedResponseCode + " but received " + conn.getResponseCode());
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
      throw new IOException("Failed to parse JSON response from API");
    }

    return podDefinition;
  }

  /**
   * Make a DELETE request to a URI
   *
   * @param expectedResponseCode the response code you expect to get from this endpoint
   */
  public void delete(Integer expectedResponseCode) throws IOException {
    HttpURLConnection conn = getUrlConnection();
    try {
      if (!NetworkUtils.sendHttpDeleteRequest(conn)) {
        throw new IOException("Failed to send delete request to " + endpointURI);
      }

      // Check the response code
      if (!NetworkUtils.checkHttpResponseCode(conn, expectedResponseCode)) {
        throw new IOException("Unexpected response from connection. Expected "
            + expectedResponseCode + " but received " + conn.getResponseCode());
      }
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Make a POST request to a URI
   *
   * @param jsonBody String form of the jsonBody
   * @param expectedResponseCode the response code you expect to get from this endpoint
   */
  public void post(String jsonBody, Integer expectedResponseCode) throws IOException {
    HttpURLConnection conn = getUrlConnection();

    try {
      // send post request with json body for the topology
      if (!NetworkUtils.sendHttpPostRequest(conn, NetworkUtils.JSON_TYPE, jsonBody.getBytes())) {
        throw new IOException("Failed to send POST to " + endpointURI);
      }

      // check the response
      if (!NetworkUtils.checkHttpResponseCode(conn, expectedResponseCode)) {
        byte[] bytes = NetworkUtils.readHttpResponse(conn);
        LOG.log(Level.SEVERE, "Failed to send POST request to endpoint");
        LOG.log(Level.SEVERE, new String(bytes));
        throw new IOException("Unexpected response from connection. Expected "
            + expectedResponseCode + " but received " + conn.getResponseCode());
      }
    } finally {
      conn.disconnect();
    }
  }
}
