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
package org.apache.heron.streamlet.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.Test;

import org.apache.heron.api.grouping.ShuffleStreamGrouping;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.utils.Utils;
import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.resource.TestBasicBolt;
import org.apache.heron.resource.TestBolt;
import org.apache.heron.resource.TestSpout;
import org.apache.heron.resource.TestWindowBolt;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Context;
import org.apache.heron.streamlet.IStreamletBasicOperator;
import org.apache.heron.streamlet.IStreamletRichOperator;
import org.apache.heron.streamlet.IStreamletWindowOperator;
import org.apache.heron.streamlet.KVStreamlet;
import org.apache.heron.streamlet.KeyedWindow;
import org.apache.heron.streamlet.SerializableConsumer;
import org.apache.heron.streamlet.SerializablePredicate;
import org.apache.heron.streamlet.SerializableTransformer;
import org.apache.heron.streamlet.Source;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.StreamletReducers;
import org.apache.heron.streamlet.WindowConfig;
import org.apache.heron.streamlet.impl.streamlets.ConsumerStreamlet;
import org.apache.heron.streamlet.impl.streamlets.CountByKeyAndWindowStreamlet;
import org.apache.heron.streamlet.impl.streamlets.CountByKeyStreamlet;
import org.apache.heron.streamlet.impl.streamlets.CustomStreamlet;
import org.apache.heron.streamlet.impl.streamlets.FilterStreamlet;
import org.apache.heron.streamlet.impl.streamlets.FlatMapStreamlet;
import org.apache.heron.streamlet.impl.streamlets.GeneralReduceByKeyStreamlet;
import org.apache.heron.streamlet.impl.streamlets.JoinStreamlet;
import org.apache.heron.streamlet.impl.streamlets.KVStreamletShadow;
import org.apache.heron.streamlet.impl.streamlets.KeyByStreamlet;
import org.apache.heron.streamlet.impl.streamlets.MapStreamlet;
import org.apache.heron.streamlet.impl.streamlets.ReduceByKeyAndWindowStreamlet;
import org.apache.heron.streamlet.impl.streamlets.ReduceByKeyStreamlet;
import org.apache.heron.streamlet.impl.streamlets.SourceStreamlet;
import org.apache.heron.streamlet.impl.streamlets.SpoutStreamlet;
import org.apache.heron.streamlet.impl.streamlets.SupplierStreamlet;
import org.apache.heron.streamlet.impl.streamlets.TransformStreamlet;
import org.apache.heron.streamlet.impl.streamlets.UnionStreamlet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link StreamletImpl}
 */
public class StreamletImplTest {

  private Builder builder = Builder.newBuilder();

  @Test
  public void testBasicParams() {
    Streamlet<Double> sample = builder.newSource(() -> Math.random());
    sample.setName("MyStreamlet");
    sample.setNumPartitions(20);
    assertEquals("MyStreamlet", sample.getName());
    assertEquals(20, sample.getNumPartitions());
    sample.setName("AnotherName");
    assertEquals("AnotherName", sample.getName());
    sample.setNumPartitions(10);
    assertEquals(10, sample.getNumPartitions());
    StreamletImpl<Double> bStreamlet = (StreamletImpl<Double>) sample;
    assertFalse(bStreamlet.isBuilt());
    assertEquals(bStreamlet.getChildren().size(), 0);
  }

  @Test
  public void testSplitAndWithStream() {
    Map<String, SerializablePredicate<Double>> splitter = new HashMap();
    splitter.put("all", i -> true);
    splitter.put("positive", i -> i > 0);
    splitter.put("negative", i -> i < 0);

    Streamlet<Double> baseStreamlet = builder.newSource(() -> Math.random());
    // The streamlet should have three output streams after split()
    Streamlet<Double> multiStreams = baseStreamlet.split(splitter);

    // Default stream is used
    Streamlet<Double> positiveStream = multiStreams.withStream("positive");
    Streamlet<Double> negativeStream = multiStreams.withStream("negative");

    Streamlet<Double> allMap = multiStreams.withStream("all").map((num) -> num * 10);
    Streamlet<Double> positiveMap = positiveStream.map((num) -> num * 10);
    Streamlet<Double> negativeMap = negativeStream.map((num) -> num * 10);

    // Original streamlet should still have the default strean id eventhough the id
    // is not available. Other shadow streamlets should have the correct stream ids.
    assertEquals(multiStreams.getStreamId(), Utils.DEFAULT_STREAM_ID);
    assertEquals(positiveStream.getStreamId(), "positive");
    assertEquals(negativeStream.getStreamId(), "negative");

    StreamletImpl<Double> impl = (StreamletImpl<Double>) multiStreams;
    assertEquals(impl.getChildren().size(), 3);

    // Children streamlets should have the right parent stream id
    assertEquals(((MapStreamlet<Double, Double>) allMap).getParent().getStreamId(),
        "all");
    assertEquals(((MapStreamlet<Double, Double>) positiveMap).getParent().getStreamId(),
        "positive");
    assertEquals(((MapStreamlet<Double, Double>) negativeMap).getParent().getStreamId(),
        "negative");
  }

  @Test(expected = RuntimeException.class)
  public void testSplitAndWithWrongStream() {
    Map<String, SerializablePredicate<Double>> splitter = new HashMap();
    splitter.put("all", i -> true);
    splitter.put("positive", i -> i > 0);
    splitter.put("negative", i -> i < 0);

    Streamlet<Double> baseStreamlet = builder.newSource(() -> Math.random());
    // The streamlet should have three output streams after split()
    Streamlet<Double> multiStreams = baseStreamlet.split(splitter);

    // Select a good stream id and a bad stream id
    Streamlet<Double> goodStream = multiStreams.withStream("positive");
    Streamlet<Double> badStream = multiStreams.withStream("wrong-id");
  }

  @Test(expected = RuntimeException.class)
  public void testWithWrongStream() {
    Streamlet<Double> baseStreamlet = builder.newSource(() -> Math.random());
    // Normal Streamlet objects, including sources, have only the default stream id.
    // Selecting any other stream using withStream() should trigger a runtime
    // exception
    Streamlet<Double> badStream = baseStreamlet.withStream("wrong-id");
  }

  @Test
  public void testSupplierStreamlet() {
    Streamlet<Double> streamlet = builder.newSource(() -> Math.random());
    assertTrue(streamlet instanceof SupplierStreamlet);
  }

  @Test
  public void testSourceStreamlet() {
    Streamlet<String> streamlet = builder.newSource(new TestSource());
    assertTrue(streamlet instanceof SourceStreamlet);
  }

  @Test
  public void testSpoutStreamlet() {
    TestSpout spout = new TestSpout();
    Streamlet<Double> streamlet = builder.newSource(spout);
    assertTrue(streamlet instanceof SpoutStreamlet);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMapStreamlet() {
    Streamlet<Double> baseStreamlet = builder.newSource(() -> Math.random());
    Streamlet<Double> streamlet = baseStreamlet.setNumPartitions(20).map((num) -> num * 10);
    assertTrue(streamlet instanceof MapStreamlet);
    MapStreamlet<Double, Double> mStreamlet = (MapStreamlet<Double, Double>) streamlet;
    assertEquals(20, mStreamlet.getNumPartitions());
    SupplierStreamlet<Double> supplierStreamlet = (SupplierStreamlet<Double>) baseStreamlet;
    assertEquals(supplierStreamlet.getChildren().size(), 1);
    assertEquals(supplierStreamlet.getChildren().get(0), streamlet);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFlatMapStreamlet() {
    Streamlet<Double> baseStreamlet = builder.newSource(() -> Math.random());
    Streamlet<Double> streamlet = baseStreamlet.setNumPartitions(20)
                                               .flatMap((num) -> Arrays.asList(num * 10));
    assertTrue(streamlet instanceof FlatMapStreamlet);
    FlatMapStreamlet<Double, Double> mStreamlet = (FlatMapStreamlet<Double, Double>) streamlet;
    assertEquals(20, mStreamlet.getNumPartitions());
    SupplierStreamlet<Double> supplierStreamlet = (SupplierStreamlet<Double>) baseStreamlet;
    assertEquals(supplierStreamlet.getChildren().size(), 1);
    assertEquals(supplierStreamlet.getChildren().get(0), streamlet);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFilterStreamlet() {
    Streamlet<Double> baseStreamlet = builder.newSource(() -> Math.random());
    Streamlet<Double> streamlet = baseStreamlet.setNumPartitions(20).filter((num) -> num != 0);
    assertTrue(streamlet instanceof FilterStreamlet);
    FilterStreamlet<Double> mStreamlet = (FilterStreamlet<Double>) streamlet;
    assertEquals(20, mStreamlet.getNumPartitions());
    SupplierStreamlet<Double> supplierStreamlet = (SupplierStreamlet<Double>) baseStreamlet;
    assertEquals(supplierStreamlet.getChildren().size(), 1);
    assertEquals(supplierStreamlet.getChildren().get(0), streamlet);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRepartitionStreamlet() {
    Streamlet<Double> baseStreamlet = builder.newSource(() -> Math.random());
    Streamlet<Double> streamlet = baseStreamlet.setNumPartitions(20).repartition(40);
    assertTrue(streamlet instanceof MapStreamlet);
    MapStreamlet<Double, Double> mStreamlet = (MapStreamlet<Double, Double>) streamlet;
    assertEquals(40, mStreamlet.getNumPartitions());
    SupplierStreamlet<Double> supplierStreamlet = (SupplierStreamlet<Double>) baseStreamlet;
    assertEquals(20, supplierStreamlet.getNumPartitions());
    assertEquals(supplierStreamlet.getChildren().size(), 1);
    assertEquals(supplierStreamlet.getChildren().get(0), streamlet);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCloneStreamlet() {
    Streamlet<Double> baseStreamlet = builder.newSource(() -> Math.random());
    List<Streamlet<Double>> streamlets = baseStreamlet.setNumPartitions(20).clone(2);
    assertEquals(streamlets.size(), 2);
    assertTrue(streamlets.get(0) instanceof MapStreamlet);
    assertTrue(streamlets.get(1) instanceof MapStreamlet);
    SupplierStreamlet<Double> supplierStreamlet = (SupplierStreamlet<Double>) baseStreamlet;
    assertEquals(20, supplierStreamlet.getNumPartitions());
    assertEquals(supplierStreamlet.getChildren().size(), 2);
    assertEquals(supplierStreamlet.getChildren(), streamlets);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testUnionStreamlet() {
    Streamlet<Double> baseStreamlet1 = builder.newSource(() -> Math.random());
    Streamlet<Double> baseStreamlet2 = builder.newSource(() -> Math.random());
    Streamlet<Double> streamlet = baseStreamlet1.union(baseStreamlet2);
    assertTrue(streamlet instanceof UnionStreamlet);
    SupplierStreamlet<Double> supplierStreamlet1 = (SupplierStreamlet<Double>) baseStreamlet1;
    SupplierStreamlet<Double> supplierStreamlet2 = (SupplierStreamlet<Double>) baseStreamlet2;
    assertEquals(supplierStreamlet1.getChildren().size(), 1);
    assertEquals(supplierStreamlet1.getChildren().get(0), streamlet);
    assertEquals(supplierStreamlet2.getChildren().size(), 1);
    assertEquals(supplierStreamlet2.getChildren().get(0), streamlet);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTransformStreamlet() {
    Streamlet<Double> baseStreamlet = builder.newSource(() -> Math.random());
    Streamlet<Double> streamlet =
        baseStreamlet.transform(new SerializableTransformer<Double, Double>() {
          @Override
          public void setup(Context context) {

          }

          @Override
          public void transform(Double aDouble, Consumer<Double> consumer) {
            consumer.accept(aDouble);
          }

          @Override
          public void cleanup() {

          }
        });
    assertTrue(streamlet instanceof TransformStreamlet);
    SupplierStreamlet<Double> supplierStreamlet = (SupplierStreamlet<Double>) baseStreamlet;
    assertEquals(supplierStreamlet.getChildren().size(), 1);
    assertEquals(supplierStreamlet.getChildren().get(0), streamlet);
  }

  private class MyBoltOperator extends TestBolt implements IStreamletRichOperator<Double, Double> {
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCustomStreamletFromBolt() {
    Streamlet<Double> baseStreamlet = builder.newSource(() -> Math.random());
    Streamlet<Double> streamlet = baseStreamlet.setNumPartitions(20)
                                               .applyOperator(new MyBoltOperator());
    assertTrue(streamlet instanceof CustomStreamlet);
    CustomStreamlet<Double, Double> mStreamlet = (CustomStreamlet<Double, Double>) streamlet;
    assertEquals(20, mStreamlet.getNumPartitions());
    SupplierStreamlet<Double> supplierStreamlet = (SupplierStreamlet<Double>) baseStreamlet;
    assertEquals(supplierStreamlet.getChildren().size(), 1);
    assertEquals(supplierStreamlet.getChildren().get(0), streamlet);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCustomStreamletWithGrouperFromBolt() throws Exception {
    Streamlet<Double> baseStreamlet = builder.newSource(() -> Math.random());
    Streamlet<Double> streamlet = baseStreamlet.setNumPartitions(20)
                                               .applyOperator(new MyBoltOperator(),
                                                              new ShuffleStreamGrouping());
    assertTrue(streamlet instanceof CustomStreamlet);
    CustomStreamlet<Double, Double> mStreamlet = (CustomStreamlet<Double, Double>) streamlet;
    assertEquals(20, mStreamlet.getNumPartitions());
    SupplierStreamlet<Double> supplierStreamlet = (SupplierStreamlet<Double>) baseStreamlet;
    assertEquals(supplierStreamlet.getChildren().size(), 1);
    assertEquals(supplierStreamlet.getChildren().get(0), streamlet);
  }

  private class MyBasicBoltOperator extends TestBasicBolt
      implements IStreamletBasicOperator<Double, Double> {
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCustomStreamletFromBasicBolt() {
    Streamlet<Double> baseStreamlet = builder.newSource(() -> Math.random());
    Streamlet<Double> streamlet = baseStreamlet.setNumPartitions(20)
                                               .applyOperator(new MyBasicBoltOperator());
    assertTrue(streamlet instanceof CustomStreamlet);
    CustomStreamlet<Double, Double> mStreamlet =
        (CustomStreamlet<Double, Double>) streamlet;
    assertEquals(20, mStreamlet.getNumPartitions());
    SupplierStreamlet<Double> supplierStreamlet = (SupplierStreamlet<Double>) baseStreamlet;
    assertEquals(supplierStreamlet.getChildren().size(), 1);
    assertEquals(supplierStreamlet.getChildren().get(0), streamlet);
  }

  private class MyWindowBoltOperator extends TestWindowBolt
      implements IStreamletWindowOperator<Double, Double> {
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCustomStreamletFromWindowBolt() {
    Streamlet<Double> baseStreamlet = builder.newSource(() -> Math.random());
    Streamlet<Double> streamlet = baseStreamlet.setNumPartitions(20)
                                               .applyOperator(new MyWindowBoltOperator());
    assertTrue(streamlet instanceof CustomStreamlet);
    CustomStreamlet<Double, Double> mStreamlet =
        (CustomStreamlet<Double, Double>) streamlet;
    assertEquals(20, mStreamlet.getNumPartitions());
    SupplierStreamlet<Double> supplierStreamlet = (SupplierStreamlet<Double>) baseStreamlet;
    assertEquals(supplierStreamlet.getChildren().size(), 1);
    assertEquals(supplierStreamlet.getChildren().get(0), streamlet);
  }

  @Test
  public void testKeyByStreamlet() {
    Streamlet<Double> baseStreamlet = builder.newSource(() -> Math.random());
    KVStreamlet<Long, Double> streamlet = baseStreamlet.keyBy(x -> Math.round(x));

    assertTrue(streamlet instanceof KVStreamletShadow);
    KVStreamletShadow<Long, Double> mStreamlet =
        (KVStreamletShadow<Long, Double>) streamlet;
    assertTrue(mStreamlet.getReal() instanceof KeyByStreamlet);
    assertEquals(1, mStreamlet.getNumPartitions());
    SupplierStreamlet<Double> supplierStreamlet = (SupplierStreamlet<Double>) baseStreamlet;
    assertEquals(supplierStreamlet.getChildren().size(), 1);
    assertEquals(supplierStreamlet.getChildren().get(0), mStreamlet.getReal());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testReduceByKeyStreamlet() {
    Streamlet<Double> baseStreamlet = builder.newSource(() -> Math.random());
    KVStreamlet<String, Double> streamlet = baseStreamlet.setNumPartitions(20)
        .<String, Double>reduceByKey(x -> (x > 0) ? "positive" : ((x < 0) ? "negative" : "zero"),
            x -> x,
            StreamletReducers::sum);

    assertTrue(streamlet instanceof KVStreamletShadow);
    KVStreamletShadow<String, Double> mStreamlet =
        (KVStreamletShadow<String, Double>) streamlet;
    assertTrue(mStreamlet.getReal() instanceof ReduceByKeyStreamlet);
    assertEquals(20, mStreamlet.getNumPartitions());
    SupplierStreamlet<Double> supplierStreamlet = (SupplierStreamlet<Double>) baseStreamlet;
    assertEquals(supplierStreamlet.getChildren().size(), 1);
    assertEquals(supplierStreamlet.getChildren().get(0), mStreamlet.getReal());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGeneralReduceByKeyStreamlet() {
    Streamlet<Double> baseStreamlet = builder.newSource(() -> Math.random());
    KVStreamlet<String, Double> streamlet = baseStreamlet.setNumPartitions(20)
        .reduceByKey(x -> (x > 0) ? "positive" : ((x < 0) ? "negative" : "zero"),
            0.0,
            StreamletReducers::sum);

    assertTrue(streamlet instanceof KVStreamletShadow);
    KVStreamletShadow<String, Double> mStreamlet =
        (KVStreamletShadow<String, Double>) streamlet;
    assertTrue(mStreamlet.getReal() instanceof GeneralReduceByKeyStreamlet);
    assertEquals(20, mStreamlet.getNumPartitions());
    SupplierStreamlet<Double> supplierStreamlet = (SupplierStreamlet<Double>) baseStreamlet;
    assertEquals(supplierStreamlet.getChildren().size(), 1);
    assertEquals(supplierStreamlet.getChildren().get(0), mStreamlet.getReal());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCountByKeyStreamlet() {
    Streamlet<Double> baseStreamlet = builder.newSource(() -> Math.random());
    KVStreamlet<String, Long> streamlet = baseStreamlet.setNumPartitions(20)
        .countByKey(x -> (x > 0) ? "positive" : ((x < 0) ? "negative" : "zero"));

    assertTrue(streamlet instanceof KVStreamletShadow);
    KVStreamletShadow<String, Long> mStreamlet =
        (KVStreamletShadow<String, Long>) streamlet;
    assertTrue(mStreamlet.getReal() instanceof CountByKeyStreamlet);
    assertEquals(20, mStreamlet.getNumPartitions());
    SupplierStreamlet<Double> supplierStreamlet = (SupplierStreamlet<Double>) baseStreamlet;
    assertEquals(supplierStreamlet.getChildren().size(), 1);
    assertEquals(supplierStreamlet.getChildren().get(0), mStreamlet.getReal());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCountByKeyAndWindowStreamlet() {
    Streamlet<Double> baseStreamlet = builder.newSource(() -> Math.random());
    KVStreamlet<KeyedWindow<String>, Long> streamlet = baseStreamlet.setNumPartitions(20)
        .countByKeyAndWindow(x -> (x > 0) ? "positive" : ((x < 0) ? "negative" : "zero"),
                             WindowConfig.TumblingCountWindow(10));

    assertTrue(streamlet instanceof KVStreamletShadow);
    KVStreamletShadow<KeyedWindow<String>, Long> mStreamlet =
        (KVStreamletShadow<KeyedWindow<String>, Long>) streamlet;
    assertTrue(mStreamlet.getReal() instanceof CountByKeyAndWindowStreamlet);
    assertEquals(20, mStreamlet.getNumPartitions());
    SupplierStreamlet<Double> supplierStreamlet = (SupplierStreamlet<Double>) baseStreamlet;
    assertEquals(supplierStreamlet.getChildren().size(), 1);
    assertEquals(supplierStreamlet.getChildren().get(0), mStreamlet.getReal());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleBuild() throws Exception {
    Streamlet<String> baseStreamlet = builder.newSource(() -> "sa re ga ma");
    baseStreamlet.flatMap(x -> Arrays.asList(x.split(" ")))
                 .reduceByKeyAndWindow(x -> x, x -> 1, WindowConfig.TumblingCountWindow(10),
                     (x, y) -> x + y);
    SupplierStreamlet<String> supplierStreamlet = (SupplierStreamlet<String>) baseStreamlet;
    assertFalse(supplierStreamlet.isBuilt());
    TopologyBuilder topologyBuilder = new TopologyBuilder();
    Set<String> stageNames = new HashSet<>();
    supplierStreamlet.build(topologyBuilder, stageNames);
    assertTrue(supplierStreamlet.isFullyBuilt());
    assertEquals(supplierStreamlet.getChildren().size(), 1);
    assertTrue(supplierStreamlet.getChildren().get(0) instanceof FlatMapStreamlet);
    FlatMapStreamlet<String, String> fStreamlet =
        (FlatMapStreamlet<String, String>) supplierStreamlet.getChildren().get(0);
    assertEquals(fStreamlet.getChildren().size(), 1);
    assertTrue(fStreamlet.getChildren().get(0) instanceof ReduceByKeyAndWindowStreamlet);
    ReduceByKeyAndWindowStreamlet<String, Integer, Integer> rStreamlet =
        (ReduceByKeyAndWindowStreamlet<String, Integer, Integer>) fStreamlet
            .getChildren().get(0);
    assertEquals(rStreamlet.getChildren().size(), 0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testComplexBuild() {
    // First source
    Streamlet<String> baseStreamlet1 = builder.newSource(() -> "sa re ga ma");
    Streamlet<String> leftStream =
        baseStreamlet1.flatMap(x -> Arrays.asList(x.split(" ")));

    // Second source
    Streamlet<String> baseStreamlet2 = builder.newSource(() -> "I Love You");
    Streamlet<String> rightStream =
        baseStreamlet2.flatMap(x -> Arrays.asList(x.split(" ")));

    // join
    leftStream.join(rightStream, x -> x, x -> x,
        WindowConfig.TumblingCountWindow(10), (x, y) -> x + y);

    SupplierStreamlet<String> supplierStreamlet1 = (SupplierStreamlet<String>) baseStreamlet1;
    SupplierStreamlet<String> supplierStreamlet2 = (SupplierStreamlet<String>) baseStreamlet2;
    assertFalse(supplierStreamlet1.isBuilt());
    assertFalse(supplierStreamlet2.isBuilt());
    TopologyBuilder topologyBuilder = new TopologyBuilder();
    Set<String> stageNames = new HashSet<>();
    supplierStreamlet1.build(topologyBuilder, stageNames);
    assertTrue(supplierStreamlet1.isBuilt());
    assertFalse(supplierStreamlet1.isFullyBuilt());

    supplierStreamlet2.build(topologyBuilder, stageNames);
    assertTrue(supplierStreamlet1.isFullyBuilt());
    assertTrue(supplierStreamlet2.isFullyBuilt());

    // go over all stuff
    assertEquals(supplierStreamlet1.getChildren().size(), 1);
    assertTrue(supplierStreamlet1.getChildren().get(0) instanceof FlatMapStreamlet);
    FlatMapStreamlet<String, String> fStreamlet =
        (FlatMapStreamlet<String, String>) supplierStreamlet1.getChildren().get(0);
    assertEquals(fStreamlet.getChildren().size(), 1);
    assertTrue(fStreamlet.getChildren().get(0) instanceof JoinStreamlet);
    JoinStreamlet<String, String, String, String> jStreamlet =
        (JoinStreamlet<String, String, String, String>) fStreamlet.getChildren().get(0);
    assertEquals(jStreamlet.getChildren().size(), 0);

    assertEquals(supplierStreamlet2.getChildren().size(), 1);
    assertTrue(supplierStreamlet2.getChildren().get(0) instanceof FlatMapStreamlet);
    fStreamlet =
        (FlatMapStreamlet<String, String>) supplierStreamlet2.getChildren().get(0);
    assertEquals(fStreamlet.getChildren().size(), 1);
    assertTrue(fStreamlet.getChildren().get(0) instanceof JoinStreamlet);
    jStreamlet =
        (JoinStreamlet<String, String, String, String>) fStreamlet.getChildren().get(0);
    assertEquals(jStreamlet.getChildren().size(), 0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCalculatedDefaultStageNames() {
    // create SupplierStreamlet
    Streamlet<String> baseStreamlet = builder.newSource(() ->
        "This is test content");
    SupplierStreamlet<String> supplierStreamlet = (SupplierStreamlet<String>) baseStreamlet;
    assertEquals(supplierStreamlet.getChildren().size(), 0);

    // apply the consumer function
    baseStreamlet.consume((SerializableConsumer<String>) s -> { });

    // build SupplierStreamlet
    assertFalse(supplierStreamlet.isBuilt());
    TopologyBuilder topologyBuilder = new TopologyBuilder();
    Set<String> stageNames = new HashSet<>();
    supplierStreamlet.build(topologyBuilder, stageNames);

    // verify SupplierStreamlet
    assertTrue(supplierStreamlet.isFullyBuilt());
    assertEquals(1, supplierStreamlet.getChildren().size());
    assertTrue(supplierStreamlet.getChildren().get(0) instanceof ConsumerStreamlet);
    assertEquals("consumer1", supplierStreamlet.getChildren().get(0).getName());

    // verify stageNames
    assertEquals(2, stageNames.size());
    List<String> expectedStageNames = Arrays.asList("consumer1", "supplier1");
    assertTrue(stageNames.containsAll(expectedStageNames));

    // verify ConsumerStreamlet
    ConsumerStreamlet<String> consumerStreamlet =
        (ConsumerStreamlet<String>) supplierStreamlet.getChildren().get(0);
    assertEquals(0, consumerStreamlet.getChildren().size());
  }

  @Test
  public void testConfigBuilderDefaultConfig() {
    Config defaultConfig = Config.defaultConfig();
    assertEquals(defaultConfig.getSerializer(), Config.Serializer.KRYO);
    assertEquals(0, Double.compare(defaultConfig.getPerContainerCpu(), -1.0));
    assertEquals(defaultConfig.getPerContainerRam(), ByteAmount.fromBytes(-1).asBytes());
    assertEquals(defaultConfig.getDeliverySemantics(), Config.DeliverySemantics.ATMOST_ONCE);

    org.apache.heron.api.Config conf = defaultConfig.getHeronConfig();
    assertFalse(conf.containsKey(org.apache.heron.api.Config.TOPOLOGY_CONTAINER_CPU_REQUESTED));
    assertFalse(conf.containsKey(org.apache.heron.api.Config.TOPOLOGY_CONTAINER_MAX_CPU_HINT));
    assertFalse(conf.containsKey(org.apache.heron.api.Config.TOPOLOGY_CONTAINER_RAM_REQUESTED));
    assertFalse(conf.containsKey(org.apache.heron.api.Config.TOPOLOGY_CONTAINER_MAX_RAM_HINT));
  }

  @Test
  public void testConfigBuilderNonDefaultConfig() {
    Config nonDefaultConfig = Config.newBuilder()
        .setDeliverySemantics(Config.DeliverySemantics.EFFECTIVELY_ONCE)
        .setSerializer(Config.Serializer.JAVA)
        .setPerContainerCpu(3.5)
        .setPerContainerRamInGigabytes(10)
        .build();
    assertEquals(nonDefaultConfig.getDeliverySemantics(),
        Config.DeliverySemantics.EFFECTIVELY_ONCE);
    assertEquals(nonDefaultConfig.getSerializer(), Config.Serializer.JAVA);
    assertEquals(nonDefaultConfig.getPerContainerRamAsGigabytes(), 10);
    assertEquals(nonDefaultConfig.getPerContainerRamAsMegabytes(), 1024 * 10);
    assertEquals(0, Double.compare(nonDefaultConfig.getPerContainerCpu(), 3.5));

    org.apache.heron.api.Config conf = nonDefaultConfig.getHeronConfig();
    assertEquals(conf.get(org.apache.heron.api.Config.TOPOLOGY_CONTAINER_CPU_REQUESTED),
                 "3.5");
    assertEquals(conf.get(org.apache.heron.api.Config.TOPOLOGY_CONTAINER_MAX_CPU_HINT),
                 "3.5");
    assertEquals(conf.get(org.apache.heron.api.Config.TOPOLOGY_CONTAINER_RAM_REQUESTED),
                 "10737418240");
    assertEquals(conf.get(org.apache.heron.api.Config.TOPOLOGY_CONTAINER_MAX_RAM_HINT),
                 "10737418240");
  }

  @Test
  public void testDefaultStreamletNameIfNotSet() {
    // create SupplierStreamlet
    Streamlet<String> baseStreamlet = builder.newSource(() ->
        "This is test content");
    SupplierStreamlet<String> supplierStreamlet = (SupplierStreamlet<String>) baseStreamlet;
    Set<String> stageNames = new HashSet<>();

    // set default name by streamlet name prefix
    supplierStreamlet.setDefaultNameIfNone(
        StreamletImpl.StreamletNamePrefix.SUPPLIER, stageNames);

    // verify stageNames
    assertEquals(1, stageNames.size());
    assertTrue(stageNames.containsAll(Arrays.asList("supplier1")));
  }

  @Test
  public void testStreamletNameIfAlreadySet() {
    String supplierName = "MyStringSupplier";
    // create SupplierStreamlet
    Streamlet<String> baseStreamlet = builder.newSource(() ->
        "This is test content");
    SupplierStreamlet<String> supplierStreamlet = (SupplierStreamlet<String>) baseStreamlet;
    supplierStreamlet.setName(supplierName);
    Set<String> stageNames = new HashSet<>();

    // set default name by streamlet name prefix
    supplierStreamlet.setDefaultNameIfNone(
        StreamletImpl.StreamletNamePrefix.SUPPLIER, stageNames);

    // verify stageNames
    assertEquals(1, stageNames.size());
    assertTrue(stageNames.containsAll(Arrays.asList(supplierName)));
  }

  @Test(expected = RuntimeException.class)
  public void testStreamletNameIfDuplicateNameIsSet() {
    // create SupplierStreamlet
    Streamlet<String> baseStreamlet = builder.newSource(() ->
        "This is test content");

    SupplierStreamlet<String> supplierStreamlet = (SupplierStreamlet<String>) baseStreamlet;

    // set duplicate streamlet name and expect thrown exception
    supplierStreamlet
        .map((content) -> content.toUpperCase()).setName("MyMapStreamlet")
        .map((content) -> content + "_test_suffix").setName("MyMapStreamlet");

    // build SupplierStreamlet
    assertFalse(supplierStreamlet.isBuilt());
    TopologyBuilder topologyBuilder = new TopologyBuilder();
    Set<String> stageNames = new HashSet<>();
    supplierStreamlet.build(topologyBuilder, stageNames);
  }

  @Test
  public void testSetNameWithInvalidValues() {
    Streamlet<Double> streamlet = builder.newSource(() -> Math.random());
    Function<String, Streamlet<Double>> function = streamlet::setName;
    testByFunction(function, null);
    testByFunction(function, "");
    testByFunction(function, "  ");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetNumPartitionsWithInvalidValue() {
    Streamlet<Double> streamlet = builder.newSource(() -> Math.random());
    streamlet.setNumPartitions(0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithStreamWithInvalidValue() {
    Streamlet<Double> baseStreamlet = builder.newSource(() -> Math.random());
    baseStreamlet.withStream("");
  }

  @Test(expected = IllegalArgumentException.class)
  @SuppressWarnings("unchecked")
  public void testCloneStreamletWithInvalidNumberOfClone() {
    Streamlet<Double> baseStreamlet = builder.newSource(() -> Math.random());
    baseStreamlet.setNumPartitions(20).clone(0);
  }

  private void testByFunction(Function<String, Streamlet<Double>> function, String sName) {
    try {
      function.apply(sName);
      fail("Should have thrown an IllegalArgumentException because streamlet name is invalid");
    } catch (IllegalArgumentException e) {
      assertEquals("Streamlet name cannot be null/blank", e.getMessage());
    }
  }

  private class TestSource implements Source<String> {

    private List<String> list;
    @Override
    public void setup(Context context) {
      list = Arrays.asList("aa", "bb", "cc");
    }

    @Override
    public Collection<String> get() {
      return list;
    }

    @Override
    public void cleanup() {
      list.clear();
    }
  }

}
