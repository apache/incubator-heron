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
package org.apache.heron.integration_test.topology.streamlet_with_split_and_with_stream;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.heron.api.Config;
import org.apache.heron.integration_test.common.AbstractTestTopology;
import org.apache.heron.integration_test.core.TestTopologyBuilder;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.SerializablePredicate;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.impl.BuilderImpl;

/**
  * Streamlet Integration Test
  */
class StreamletWithSplitAndWithStream extends AbstractTestTopology {
  private static AtomicInteger atomicInteger = new AtomicInteger(-3);

  StreamletWithSplitAndWithStream(String[] args) throws MalformedURLException {
    super(args);
  }

  @Override
  protected TestTopologyBuilder buildTopology(TestTopologyBuilder testTopologyBuilder) {
    Map<String, SerializablePredicate<Integer>> splitter = new HashMap();
    splitter.put("all", i -> true);
    splitter.put("positive", i -> i > 0);
    splitter.put("negative", i -> i < 0);

    Builder streamletBuilder = Builder.newBuilder();
    Streamlet<Integer> multi = streamletBuilder
        .newSource(() -> atomicInteger.getAndIncrement())
        .setName("incremented-numbers-from--3")
        .filter(i -> i <= 4)
        .setName("numbers-lower-than-4")
        .split(splitter)
        .setName("split");

    multi.withStream("all").map((Integer i) -> String.format("all_%d", i));
    multi.withStream("positive").map((Integer i) -> String.format("pos_%d", i));
    multi.withStream("negative").map((Integer i) -> String.format("neg_%d", i));

    BuilderImpl streamletBuilderImpl = (BuilderImpl) streamletBuilder;
    TestTopologyBuilder topology =
        (TestTopologyBuilder) streamletBuilderImpl.build(testTopologyBuilder);

    return topology;
  }

  public static void main(String[] args) throws Exception {
    Config conf = new Config();
    StreamletWithSplitAndWithStream topology = new StreamletWithSplitAndWithStream(args);
    topology.submit(conf);
  }
}
