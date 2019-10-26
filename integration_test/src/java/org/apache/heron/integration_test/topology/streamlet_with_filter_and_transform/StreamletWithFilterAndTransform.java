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
package org.apache.heron.integration_test.topology.streamlet_with_filter_and_transform;

import java.net.MalformedURLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.heron.api.Config;
import org.apache.heron.integration_test.common.AbstractTestTopology;
import org.apache.heron.integration_test.core.TestTopologyBuilder;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.SerializableTransformer;
import org.apache.heron.streamlet.impl.BuilderImpl;

/**
  * Streamlet Integration Test
  */
class StreamletWithFilterAndTransform extends AbstractTestTopology {

  StreamletWithFilterAndTransform(String[] args) throws MalformedURLException {
    super(args);
  }

  @Override
  protected TestTopologyBuilder buildTopology(TestTopologyBuilder testTopologyBuilder) {
    AtomicInteger atomicInteger = new AtomicInteger(0);

    Builder streamletBuilder = Builder.newBuilder();
    streamletBuilder
      .newSource(() -> atomicInteger.getAndIncrement())
      .setName("incremented-numbers")
      .filter(i -> i <= 7)
      .setName("numbers-lower-than-8")
      .transform(new TextTransformer())
      .setName("numbers-transformed-to-text");

    BuilderImpl streamletBuilderImpl = (BuilderImpl) streamletBuilder;
    TestTopologyBuilder topology =
        (TestTopologyBuilder) streamletBuilderImpl.build(testTopologyBuilder);

    return topology;
  }

  public static class TextTransformer implements SerializableTransformer<Integer, String> {
    private String[] alphabet;

    @Override
    public void setup(Context context) {
      alphabet = new String[] {"a", "b", "c", "d", "e", "f", "g", "h"};
    }

    @Override
    public void transform(Integer i, Consumer<String> fun) {
      fun.accept(String.format("%s-%d", alphabet[i].toUpperCase(), i));
    }

    @Override
    public void cleanup() { }
  }

  public static void main(String[] args) throws Exception {
    Config conf = new Config();
    StreamletWithFilterAndTransform topology = new StreamletWithFilterAndTransform(args);
    topology.submit(conf);
  }
}
