//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.streamlet.impl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.Test;

import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.streamlet.Config;
import com.twitter.heron.streamlet.Context;
import com.twitter.heron.streamlet.Resources;
import com.twitter.heron.streamlet.SerializableConsumer;
import com.twitter.heron.streamlet.SerializableTransformer;
import com.twitter.heron.streamlet.Streamlet;
import com.twitter.heron.streamlet.WindowConfig;
import com.twitter.heron.streamlet.impl.streamlets.ConsumerStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.FilterStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.FlatMapStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.JoinStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.MapStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.ReduceByKeyAndWindowStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.SupplierStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.TransformStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.UnionStreamlet;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link StreamletImpl}
 */
public class StreamletImplTest {

  @Test
  public void testBasicParams() throws Exception {
    Streamlet<Double> sample = StreamletImpl.createSupplierStreamlet(() -> Math.random());
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
  public void testSupplierStreamlet() throws Exception {
    Streamlet<Double> streamlet = StreamletImpl.createSupplierStreamlet(() -> Math.random());
    assertTrue(streamlet instanceof SupplierStreamlet);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMapStreamlet() throws Exception {
    Streamlet<Double> baseStreamlet = StreamletImpl.createSupplierStreamlet(() -> Math.random());
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
  public void testFlatMapStreamlet() throws Exception {
    Streamlet<Double> baseStreamlet = StreamletImpl.createSupplierStreamlet(() -> Math.random());
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
  public void testFilterStreamlet() throws Exception {
    Streamlet<Double> baseStreamlet = StreamletImpl.createSupplierStreamlet(() -> Math.random());
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
  public void testRepartitionStreamlet() throws Exception {
    Streamlet<Double> baseStreamlet = StreamletImpl.createSupplierStreamlet(() -> Math.random());
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
  public void testCloneStreamlet() throws Exception {
    Streamlet<Double> baseStreamlet = StreamletImpl.createSupplierStreamlet(() -> Math.random());
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
  public void testUnionStreamlet() throws Exception {
    Streamlet<Double> baseStreamlet1 = StreamletImpl.createSupplierStreamlet(() -> Math.random());
    Streamlet<Double> baseStreamlet2 = StreamletImpl.createSupplierStreamlet(() -> Math.random());
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
  public void testTransformStreamlet() throws Exception {
    Streamlet<Double> baseStreamlet = StreamletImpl.createSupplierStreamlet(() -> Math.random());
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

  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleBuild() throws Exception {
    Streamlet<String> baseStreamlet = StreamletImpl.createSupplierStreamlet(() -> "sa re ga ma");
    baseStreamlet.flatMap(x -> Arrays.asList(x.split(" ")))
                 .reduceByKeyAndWindow(x -> x, x -> 1, WindowConfig.TumblingCountWindow(10),
                     (x, y) -> x + y);
    SupplierStreamlet<String> supplierStreamlet = (SupplierStreamlet<String>) baseStreamlet;
    assertFalse(supplierStreamlet.isBuilt());
    TopologyBuilder builder = new TopologyBuilder();
    Set<String> stageNames = new HashSet<>();
    supplierStreamlet.build(builder, stageNames);
    assertTrue(supplierStreamlet.allBuilt());
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
  public void testComplexBuild() throws Exception {
    // First source
    Streamlet<String> baseStreamlet1 = StreamletImpl.createSupplierStreamlet(() -> "sa re ga ma");
    Streamlet<String> leftStream =
        baseStreamlet1.flatMap(x -> Arrays.asList(x.split(" ")));

    // Second source
    Streamlet<String> baseStreamlet2 = StreamletImpl.createSupplierStreamlet(() -> "I Love You");
    Streamlet<String> rightStream =
        baseStreamlet2.flatMap(x -> Arrays.asList(x.split(" ")));

    // join
    leftStream.join(rightStream, x -> x, x -> x,
        WindowConfig.TumblingCountWindow(10), (x, y) -> x + y);

    SupplierStreamlet<String> supplierStreamlet1 = (SupplierStreamlet<String>) baseStreamlet1;
    SupplierStreamlet<String> supplierStreamlet2 = (SupplierStreamlet<String>) baseStreamlet2;
    assertFalse(supplierStreamlet1.isBuilt());
    assertFalse(supplierStreamlet2.isBuilt());
    TopologyBuilder builder = new TopologyBuilder();
    Set<String> stageNames = new HashSet<>();
    supplierStreamlet1.build(builder, stageNames);
    assertTrue(supplierStreamlet1.isBuilt());
    assertFalse(supplierStreamlet1.allBuilt());

    supplierStreamlet2.build(builder, stageNames);
    assertTrue(supplierStreamlet1.allBuilt());
    assertTrue(supplierStreamlet2.allBuilt());

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
    Streamlet<String> baseStreamlet = StreamletImpl.createSupplierStreamlet(() ->
        "This is test content");
    SupplierStreamlet<String> supplierStreamlet = (SupplierStreamlet<String>) baseStreamlet;
    assertEquals(supplierStreamlet.getChildren().size(), 0);

    // apply the consumer function
    baseStreamlet.consume((SerializableConsumer<String>) s -> { });

    // build SupplierStreamlet
    assertFalse(supplierStreamlet.isBuilt());
    TopologyBuilder builder = new TopologyBuilder();
    Set<String> stageNames = new HashSet<>();
    supplierStreamlet.build(builder, stageNames);

    // verify SupplierStreamlet
    assertTrue(supplierStreamlet.allBuilt());
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
  public void testResourcesBuilder() {
    Resources defaultResources = Resources.defaultResources();
    assertEquals(0, Float.compare(defaultResources.getCpu(), 1.0f));
    assertEquals(defaultResources.getRam(), ByteAmount.fromMegabytes(100));

    Resources nonDefaultResources = new Resources.Builder()
        .setCpu(5.1f)
        .setRamInGB(20)
        .build();
    assertEquals(0, Float.compare(nonDefaultResources.getCpu(), 5.1f));
    assertEquals(nonDefaultResources.getRam(), ByteAmount.fromGigabytes(20));
  }

  @Test
  public void testConfigBuilder() {
    Config defaultConfig = Config.defaultConfig();
    assertEquals(defaultConfig.getDeliverySemantics(), Config.DeliverySemantics.ATMOST_ONCE);
    assertEquals(defaultConfig.getNumContainers(), 1);
    assertEquals(0, Float.compare(defaultConfig.getResources().getCpu(), 1.0f));
    assertEquals(defaultConfig.getResources().getRam(), ByteAmount.fromMegabytes(100));
    assertEquals(defaultConfig.getSerializer(), Config.Serializer.KRYO);

    Resources nonDefaultResources = new Resources.Builder()
        .setCpu(3.1f)
        .setRamInMB(2500)
        .build();

    Config nonDefaultConfig = new Config.Builder()
        .setContainerResources(nonDefaultResources)
        .setDeliverySemantics(Config.DeliverySemantics.EFFECTIVELY_ONCE)
        .setNumContainers(8)
        .setTopologySerializer(Config.Serializer.JAVA)
        .setUserConfig("key", "value")
        .build();
    assertEquals(nonDefaultConfig.getNumContainers(), 8);
    assertEquals(nonDefaultConfig.getDeliverySemantics(),
        Config.DeliverySemantics.EFFECTIVELY_ONCE);
    assertEquals(0, Float.compare(nonDefaultConfig.getResources().getCpu(), 3.1f));
    assertEquals(nonDefaultConfig.getResources().getRam(), ByteAmount.fromMegabytes(2500));
    assertEquals(nonDefaultConfig.getSerializer(), Config.Serializer.JAVA);

    Config multiSetConfig = new Config.Builder()
        .setTopologySerializer(Config.Serializer.JAVA)
        .setTopologySerializer(Config.Serializer.KRYO)
        .build();
    assertEquals(multiSetConfig.getSerializer(), Config.Serializer.KRYO);
  }

  @Test
  public void testSetNameWithInvalidValues() {
    Streamlet<Double> streamlet = StreamletImpl.createSupplierStreamlet(() -> Math.random());
    Function<String, Streamlet<Double>> function = streamlet::setName;
    testByFunction(function, null);
    testByFunction(function, "");
    testByFunction(function, "  ");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetNumPartitionsWithInvalidValue() {
    Streamlet<Double> streamlet = StreamletImpl.createSupplierStreamlet(() -> Math.random());
    streamlet.setNumPartitions(0);
  }

  private void testByFunction(Function<String, Streamlet<Double>> function, String sName) {
    try {
      function.apply(sName);
      fail("Should have thrown an IllegalArgumentException because streamlet name is invalid");
    } catch (IllegalArgumentException e) {
      assertEquals("Streamlet name cannot be null/blank", e.getMessage());
    }
  }

}
