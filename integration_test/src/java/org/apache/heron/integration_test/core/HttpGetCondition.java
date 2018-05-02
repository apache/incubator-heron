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

package org.apache.heron.integration_test.core;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Class that satisfies a condition once it gets a 200 response from an http get request
 */
class HttpGetCondition implements Condition {
  private static final long serialVersionUID = -3370730083374050883L;
  private static final long SLEEP_MS = 10 * 1000;
  private static final int MAX_ATTEMPTS = 40;
  private URL url;

  HttpGetCondition(String urlString) {
    try {
      this.url = new URL(urlString);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public void satisfyCondition() {
    HttpURLConnection connection = null;
    try {
      for (int attempts = 0; attempts < MAX_ATTEMPTS; attempts++) {
        connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.connect();
        if (connection.getResponseCode() == 200) {
          return;
        }
        connection.disconnect();
        Thread.sleep(SLEEP_MS);
      }
      throw new RuntimeException(String.format(
          "Failed to satisfy http get condition to url %s after %d attempts", url, MAX_ATTEMPTS));
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }

  }
}
