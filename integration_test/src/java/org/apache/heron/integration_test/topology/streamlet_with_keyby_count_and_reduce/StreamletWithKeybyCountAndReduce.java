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
package org.apache.heron.integration_test.topology.streamlet_with_keyby_count_and_reduce;

import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.heron.api.Config;
import org.apache.heron.integration_test.common.AbstractTestTopology;
import org.apache.heron.integration_test.core.TestTopologyBuilder;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.SerializableFunction;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.StreamletReducers;
import org.apache.heron.streamlet.impl.BuilderImpl;

class StreamletWithKeybyCountAndReduce extends AbstractTestTopology {
  private static final String MONTHS = "january - february - march - april - may - june"
      + " - july - august - september - october - november - december";
  private static final Set<String> SPRING_MONTHS =
      new HashSet<>(Arrays.asList("march", "april", "may"));
  private static final Set<String> SUMMER_MONTHS =
      new HashSet<>(Arrays.asList("june", "july", "august"));
  private static final Set<String> FALL_MONTHS =
      new HashSet<>(Arrays.asList("september", "october", "november"));
  private static final Set<String> WINTER_MONTHS =
      new HashSet<>(Arrays.asList("december", "january", "february"));
  private static Set<String> incomingMonths = new HashSet<>();

  StreamletWithKeybyCountAndReduce(String[] args) throws MalformedURLException {
    super(args);
  }

  @Override
  protected TestTopologyBuilder buildTopology(TestTopologyBuilder testTopologyBuilder) {
    Builder streamletBuilder = Builder.newBuilder();

    Streamlet<String> monthStreamlet = streamletBuilder
        .newSource(() -> MONTHS)
        .setName("months-text")
        .flatMap((String m) -> Arrays.asList(m.split(" - ")))
        .setName("months")
        // Make sure each month is emitted only once
        .filter((month) -> incomingMonths.add(month.toLowerCase()))
        .setName("unique-months");

    SerializableFunction<String, String> getSeason = month -> {
      if (SPRING_MONTHS.contains(month)) {
        return "spring";
      } else if (SUMMER_MONTHS.contains(month)) {
        return "summer";
      } else if (FALL_MONTHS.contains(month)) {
        return "fall";
      } else if (WINTER_MONTHS.contains(month)) {
        return "winter";
      } else {
        return "really?";
      }
    };

    SerializableFunction<String, Integer> getNumberOfDays = month -> {
      switch (month) {
        case "january":
          return 31;
        case "february":
          return 28;   // Dont use this code in real projects
        case "march":
          return 31;
        case "april":
          return 30;
        case "may":
          return 31;
        case "june":
          return 30;
        case "july":
          return 31;
        case "august":
          return 31;
        case "september":
          return 30;
        case "october":
          return 31;
        case "november":
          return 30;
        case "december":
          return 31;
        default:
          return -1;  // Shouldn't be here
      }
    };

    // Count months per season
    monthStreamlet
        .keyBy(getSeason, getNumberOfDays)
        .setName("key-by-season")
        .countByKey(x -> x.getKey())
        .setName("key-by-and-count")
        .map(x -> String.format("%s: %d months", x.getKey(), x.getValue()))
        .setName("to-string");

    // Sum days per season
    monthStreamlet
        .<String, Integer>reduceByKey(getSeason, getNumberOfDays, StreamletReducers::sum)
        .setName("sum-by-season")
        .map(x -> String.format("%s: %d days", x.getKey(), x.getValue()))
        .setName("to-string-2");

    BuilderImpl streamletBuilderImpl = (BuilderImpl) streamletBuilder;
    TestTopologyBuilder topology =
        (TestTopologyBuilder) streamletBuilderImpl.build(testTopologyBuilder);

    return topology;
  }

  public static void main(String[] args) throws Exception {
    Config conf = new Config();
    StreamletWithKeybyCountAndReduce topology =
        new StreamletWithKeybyCountAndReduce(args);
    topology.submit(conf);
  }
}
