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
package org.apache.heron.integration_topology_test.core;

import java.io.IOException;
import java.text.ParseException;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

public final class HttpUtils {

  private HttpUtils() { }

  public static int httpJsonPost(String newHttpPostUrl, String jsonData)
      throws IOException, ParseException {
    HttpClient client = HttpClientBuilder.create().build();
    HttpPost post = new HttpPost(newHttpPostUrl);

    StringEntity requestEntity = new StringEntity(jsonData, ContentType.APPLICATION_JSON);

    post.setEntity(requestEntity);
    HttpResponse response = client.execute(post);

    return response.getStatusLine().getStatusCode();
  }

  public static void postToHttpServer(String postUrl, String data, String dataName)
    throws RuntimeException {
    try {
      int responseCode = -1;
      for (int attempts = 0; attempts < 2; attempts++) {
        responseCode = httpJsonPost(postUrl, data);
        if (responseCode == 200) {
          return;
        }
      }
      throw new RuntimeException(
          String.format("Failed to post %s to %s: %s",
              dataName, postUrl, responseCode));
    } catch (IOException | java.text.ParseException e) {
      throw new RuntimeException(String.format("Posting %s to %s failed",
          dataName, postUrl), e);
    }
  }
}
