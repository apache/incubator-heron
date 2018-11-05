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
package org.apache.heron.integration_test.topology.streamlet_with_map_and_flatmap_and_filter_and_clone;

import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.heron.api.Config;
import org.apache.heron.integration_test.common.AbstractTestTopology;
import org.apache.heron.integration_test.core.TestTopologyBuilder;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.impl.BuilderImpl;

class StreamletWithMapAndFlatMapAndFilterAndClone extends AbstractTestTopology {
  private static final String MONTHS = "january - february - march - april - may - june"
      + " - july - august - september - october - november - december";
  private static final Set<String> SUMMER_MONTHS =
      new HashSet<>(Arrays.asList("june", "july", "august"));
  private static Set<String> incomingMonths = new HashSet<>();

  StreamletWithMapAndFlatMapAndFilterAndClone(String[] args) throws MalformedURLException {
    super(args);
  }

  @Override
  protected TestTopologyBuilder buildTopology(TestTopologyBuilder testTopologyBuilder) {
    Builder streamletBuilder = Builder.newBuilder();

    Streamlet<String> streamlet = streamletBuilder
        .newSource(() -> MONTHS)
        .setName("months-text")
        .flatMap((String m) -> Arrays.asList(m.split(" - ")))
        .setName("months")
        .filter((month) ->
            SUMMER_MONTHS.contains(month.toLowerCase()) && incomingMonths.add(month.toLowerCase()))
        .setName("summer-months")
        .map((String word) -> word.substring(0, 3))
        .setName("summer-months-with-short-name");

    List<Streamlet<String>> clonedStreamlet = streamlet.clone(2);

    //Returns Summer Months with year
    clonedStreamlet.get(0).map((String month) -> month + "_2018");

    //Returns Summer Months with Uppercase
    clonedStreamlet.get(1).map((String month) -> month.toUpperCase());

    BuilderImpl streamletBuilderImpl = (BuilderImpl) streamletBuilder;
    TestTopologyBuilder topology =
        (TestTopologyBuilder) streamletBuilderImpl.build(testTopologyBuilder);

    return topology;
  }

  public static void main(String[] args) throws Exception {
    Config conf = new Config();
    StreamletWithMapAndFlatMapAndFilterAndClone topology =
        new StreamletWithMapAndFlatMapAndFilterAndClone(args);
    topology.submit(conf);
  }
}
