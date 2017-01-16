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

package com.twitter.heron.metricscachemgr.metricscache;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MetricsCacheTest {
  public static final String CONFIG_PATH =
      "../../../../../../../../../heron/config/src/yaml/conf/examples/metrics_sinks.yaml";
  private static String debugFilePath =
      "/tmp/" + MetricsCacheTest.class.getSimpleName() + ".debug.txt";
  private MetricsCache mc = null;
  private Path file = null;
  private List<String> lines = null;

  @Before
  public void before() throws Exception {
    file = Paths.get(debugFilePath);
    lines = new ArrayList<>();

    lines.add(Paths.get(".").toAbsolutePath().normalize().toString());

  }

  @After
  public void after() throws IOException {
    Files.write(file, lines, Charset.forName("UTF-8"));
  }

  @Test
  public void testMetricCache() {

  }
}
