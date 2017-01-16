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

package com.twitter.heron.metricscachemgr;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.statemgr.localfs.LocalFileSystemStateManager;

/**
 * example: query metricscache
 */
public final class ExampleMetricsQuery {
  private ExampleMetricsQuery() {
  }

  public static void main(String[] args)
      throws ExecutionException, InterruptedException, IOException {
    if (args.length < 3) {
      System.out.println(
          "Usage: java MetricsQuery <topology_name> <component_name> <metrics_name>");
    } else {
      System.out.println("topology: " + args[0] + "; component: " + args[1]);
    }

    Config config = Config.newBuilder()
        .put(Keys.stateManagerRootPath(),
            System.getProperty("user.home") + "/.herondata/repository/state/local")
        .build();
    LocalFileSystemStateManager stateManager = new LocalFileSystemStateManager();
    stateManager.initialize(config);

    TopologyMaster.MetricsCacheLocation location =
        stateManager.getMetricsCacheLocation(null, args[0]).get();
    if (location == null || !location.isInitialized()) {
      System.out.println("MetricsCacheMgr is not ready");
      return;
    }
//    System.out.println("location: " + location);

    //----------------

    String url = "http://" + location.getHost() + ":" + location.getStatsPort()
        + MetricsCacheManagerHttpServer.PATH_STATS;
//    System.out.println("url: " + url);

    URL obj = new URL(url);
    HttpURLConnection con = (HttpURLConnection) obj.openConnection();

    //add reuqest header
    con.setRequestMethod("POST");

    con.setDoOutput(true);
    OutputStream out = con.getOutputStream();
    TopologyMaster.MetricRequest.newBuilder()
        .setComponentName(args[1])
        .setMinutely(true)
        .setInterval(-1)
        .addAllMetric(Arrays.asList(Arrays.copyOfRange(args, 2, args.length)))
        .build()
        .writeTo(out);
    out.close();

    // response code
    int responseCode = con.getResponseCode();
    System.out.println("\nSending 'POST' request to URL : " + url);
    System.out.println("Response Code : " + responseCode);

    // response content
    InputStream in = con.getInputStream();
    TopologyMaster.MetricResponse response = TopologyMaster.MetricResponse.parseFrom(in);
    in.close();

    //print result
    System.out.println(response.toString());
  }

}

