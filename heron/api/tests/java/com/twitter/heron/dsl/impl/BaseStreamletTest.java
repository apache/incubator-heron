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
package com.twitter.heron.dsl.impl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.dsl.Context;
import com.twitter.heron.dsl.KVStreamlet;
import com.twitter.heron.dsl.KeyValue;
import com.twitter.heron.dsl.SerializableTransformer;
import com.twitter.heron.dsl.Streamlet;
import com.twitter.heron.dsl.Window;
import com.twitter.heron.dsl.WindowConfig;
import com.twitter.heron.dsl.impl.streamlets.FilterStreamlet;
import com.twitter.heron.dsl.impl.streamlets.FlatMapStreamlet;
import com.twitter.heron.dsl.impl.streamlets.JoinStreamlet;
import com.twitter.heron.dsl.impl.streamlets.KVFlatMapStreamlet;
import com.twitter.heron.dsl.impl.streamlets.KVMapStreamlet;
import com.twitter.heron.dsl.impl.streamlets.MapStreamlet;
import com.twitter.heron.dsl.impl.streamlets.ReduceByKeyAndWindowStreamlet;
import com.twitter.heron.dsl.impl.streamlets.ReduceByWindowStreamlet;
import com.twitter.heron.dsl.impl.streamlets.SupplierStreamlet;
import com.twitter.heron.dsl.impl.streamlets.TransformStreamlet;
import com.twitter.heron.dsl.impl.streamlets.UnionStreamlet;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link BaseStreamlet}
 */
public class BaseStreamletTest {

  private <T> boolean isFullyBuilt(BaseStreamlet<T> streamlet) {
    if (!streamlet.isBuilt()) {
      return false;
    }
    for (Streamlet<?> child : streamlet.getChildren()) {
      BaseStreamlet<?> aChild = (BaseStreamlet<?>) child;
      if (!isFullyBuilt(aChild)) {
        return false;
      }
    }
    return true;
  }

  @Before
  public void setUp() {
  }

  @Test
  public void testBasicParams() throws Exception {
    Streamlet<Double> sample = BaseStreamlet.createSupplierStreamlet(() -> Math.random());
    sample.setName("MyStreamlet");
    sample.setNumPartitions(20);
    assertEquals("MyStreamlet", sample.getName());
    assertEquals(20, sample.getNumPartitions());
    sample.setName("AnotherName");
    assertEquals("AnotherName", sample.getName());
    sample.setNumPartitions(10);
    assertEquals(10, sample.getNumPartitions());
    BaseStreamlet<Double> bStreamlet = (BaseStreamlet<Double>) sample;
    assertFalse(bStreamlet.isBuilt());
    assertEquals(bStreamlet.getChildren().size(), 0);
  }

  @Test
  public void testSupplierStreamlet() throws Exception {
    Streamlet<Double> streamlet = BaseStreamlet.createSupplierStreamlet(() -> Math.random());
    assertTrue(streamlet instanceof SupplierStreamlet);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMapStreamlet() throws Exception {
    Streamlet<Double> baseStreamlet = BaseStreamlet.createSupplierStreamlet(() -> Math.random());
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
    Streamlet<Double> baseStreamlet = BaseStreamlet.createSupplierStreamlet(() -> Math.random());
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
  public void testMapToKVStreamlet() throws Exception {
    Streamlet<Double> baseStreamlet = BaseStreamlet.createSupplierStreamlet(() -> Math.random());
    KVStreamlet<Double, Double> streamlet = baseStreamlet.setNumPartitions(20)
        .mapToKV((num) -> new KeyValue<>(num, num));
    assertTrue(streamlet instanceof KVMapStreamlet);
    KVMapStreamlet<Double, Double, Double> mStreamlet =
        (KVMapStreamlet<Double, Double, Double>) streamlet;
    assertEquals(20, mStreamlet.getNumPartitions());
    SupplierStreamlet<Double> supplierStreamlet = (SupplierStreamlet<Double>) baseStreamlet;
    assertEquals(supplierStreamlet.getChildren().size(), 1);
    assertEquals(supplierStreamlet.getChildren().get(0), streamlet);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFlatMapToKVStreamlet() throws Exception {
    Streamlet<Double> baseStreamlet = BaseStreamlet.createSupplierStreamlet(() -> Math.random());
    KVStreamlet<Double, Double> streamlet = baseStreamlet.setNumPartitions(20)
        .flatMapToKV((num) -> Arrays.asList(new KeyValue<>(num, num)));
    assertTrue(streamlet instanceof KVFlatMapStreamlet);
    KVFlatMapStreamlet<Double, Double, Double> mStreamlet =
        (KVFlatMapStreamlet<Double, Double, Double>) streamlet;
    assertEquals(20, mStreamlet.getNumPartitions());
    SupplierStreamlet<Double> supplierStreamlet = (SupplierStreamlet<Double>) baseStreamlet;
    assertEquals(supplierStreamlet.getChildren().size(), 1);
    assertEquals(supplierStreamlet.getChildren().get(0), streamlet);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFilterStreamlet() throws Exception {
    Streamlet<Double> baseStreamlet = BaseStreamlet.createSupplierStreamlet(() -> Math.random());
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
    Streamlet<Double> baseStreamlet = BaseStreamlet.createSupplierStreamlet(() -> Math.random());
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
    Streamlet<Double> baseStreamlet = BaseStreamlet.createSupplierStreamlet(() -> Math.random());
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
    Streamlet<Double> baseStreamlet1 = BaseStreamlet.createSupplierStreamlet(() -> Math.random());
    Streamlet<Double> baseStreamlet2 = BaseStreamlet.createSupplierStreamlet(() -> Math.random());
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
  public void testReduceByWindowStreamlet() throws Exception {
    Streamlet<Double> baseStreamlet = BaseStreamlet.createSupplierStreamlet(() -> Math.random());
    KVStreamlet<Window, Double> streamlet =
        baseStreamlet.reduceByWindow(WindowConfig.TumblingCountWindow(10), (x, y) -> x + y);
    assertTrue(streamlet instanceof ReduceByWindowStreamlet);
    SupplierStreamlet<Double> supplierStreamlet = (SupplierStreamlet<Double>) baseStreamlet;
    assertEquals(supplierStreamlet.getChildren().size(), 1);
    assertEquals(supplierStreamlet.getChildren().get(0), streamlet);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTransformStreamlet() throws Exception {
    Streamlet<Double> baseStreamlet = BaseStreamlet.createSupplierStreamlet(() -> Math.random());
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
    Streamlet<String> baseStreamlet = BaseStreamlet.createSupplierStreamlet(() -> "sa re ga ma");
    baseStreamlet.flatMap(x -> Arrays.asList(x.split(" ")))
                 .mapToKV(x -> new KeyValue<>(x, 1))
                 .reduceByKeyAndWindow(WindowConfig.TumblingCountWindow(10), (x, y) -> x + y);
    SupplierStreamlet<String> supplierStreamlet = (SupplierStreamlet<String>) baseStreamlet;
    assertFalse(supplierStreamlet.isBuilt());
    TopologyBuilder builder = new TopologyBuilder();
    Set<String> stageNames = new HashSet<>();
    supplierStreamlet.build(builder, stageNames);
    assertTrue(isFullyBuilt(supplierStreamlet));
    assertEquals(supplierStreamlet.getChildren().size(), 1);
    assertTrue(supplierStreamlet.getChildren().get(0) instanceof FlatMapStreamlet);
    FlatMapStreamlet<String, String> fStreamlet =
        (FlatMapStreamlet<String, String>) supplierStreamlet.getChildren().get(0);
    assertEquals(fStreamlet.getChildren().size(), 1);
    assertTrue(fStreamlet.getChildren().get(0) instanceof KVMapStreamlet);
    KVMapStreamlet<String, String, Integer> mkvStreamlet =
        (KVMapStreamlet<String, String, Integer>) fStreamlet.getChildren().get(0);
    assertEquals(mkvStreamlet.getChildren().size(), 1);
    assertTrue(mkvStreamlet.getChildren().get(0) instanceof ReduceByKeyAndWindowStreamlet);
    ReduceByKeyAndWindowStreamlet<String, Integer> rStreamlet =
        (ReduceByKeyAndWindowStreamlet<String, Integer>) mkvStreamlet.getChildren().get(0);
    assertEquals(rStreamlet.getChildren().size(), 0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testComplexBuild() throws Exception {
    // First source
    Streamlet<String> baseStreamlet1 = BaseStreamlet.createSupplierStreamlet(() -> "sa re ga ma");
    KVStreamlet<String, Integer> leftStream =
        baseStreamlet1.flatMap(x -> Arrays.asList(x.split(" ")))
        .mapToKV(x -> new KeyValue<>(x, 1));

    // Second source
    Streamlet<String> baseStreamlet2 = BaseStreamlet.createSupplierStreamlet(() -> "I Love You");
    KVStreamlet<String, Integer> rightStream =
        baseStreamlet2.flatMap(x -> Arrays.asList(x.split(" ")))
        .mapToKV(x -> new KeyValue<>(x, 1));

    // join
    leftStream.join(rightStream, WindowConfig.TumblingCountWindow(10), (x, y) -> x + y);

    SupplierStreamlet<String> supplierStreamlet1 = (SupplierStreamlet<String>) baseStreamlet1;
    SupplierStreamlet<String> supplierStreamlet2 = (SupplierStreamlet<String>) baseStreamlet2;
    assertFalse(supplierStreamlet1.isBuilt());
    assertFalse(supplierStreamlet2.isBuilt());
    TopologyBuilder builder = new TopologyBuilder();
    Set<String> stageNames = new HashSet<>();
    supplierStreamlet1.build(builder, stageNames);
    assertTrue(supplierStreamlet1.isBuilt());
    assertFalse(isFullyBuilt(supplierStreamlet1));

    supplierStreamlet2.build(builder, stageNames);
    assertTrue(isFullyBuilt(supplierStreamlet1));
    assertTrue(isFullyBuilt(supplierStreamlet2));

    // go over all stuff
    assertEquals(supplierStreamlet1.getChildren().size(), 1);
    assertTrue(supplierStreamlet1.getChildren().get(0) instanceof FlatMapStreamlet);
    FlatMapStreamlet<String, String> fStreamlet =
        (FlatMapStreamlet<String, String>) supplierStreamlet1.getChildren().get(0);
    assertEquals(fStreamlet.getChildren().size(), 1);
    assertTrue(fStreamlet.getChildren().get(0) instanceof KVMapStreamlet);
    KVMapStreamlet<String, String, Integer> mkvStreamlet =
        (KVMapStreamlet<String, String, Integer>) fStreamlet.getChildren().get(0);
    assertEquals(mkvStreamlet.getChildren().size(), 1);
    assertTrue(mkvStreamlet.getChildren().get(0) instanceof JoinStreamlet);
    JoinStreamlet<String, Integer, Integer, Integer> jStreamlet =
        (JoinStreamlet<String, Integer, Integer, Integer>) mkvStreamlet.getChildren().get(0);
    assertEquals(jStreamlet.getChildren().size(), 0);

    assertEquals(supplierStreamlet2.getChildren().size(), 1);
    assertTrue(supplierStreamlet2.getChildren().get(0) instanceof FlatMapStreamlet);
    fStreamlet =
        (FlatMapStreamlet<String, String>) supplierStreamlet2.getChildren().get(0);
    assertEquals(fStreamlet.getChildren().size(), 1);
    assertTrue(fStreamlet.getChildren().get(0) instanceof KVMapStreamlet);
    mkvStreamlet =
        (KVMapStreamlet<String, String, Integer>) fStreamlet.getChildren().get(0);
    assertEquals(mkvStreamlet.getChildren().size(), 1);
    assertTrue(mkvStreamlet.getChildren().get(0) instanceof JoinStreamlet);
    jStreamlet =
        (JoinStreamlet<String, Integer, Integer, Integer>) mkvStreamlet.getChildren().get(0);
    assertEquals(jStreamlet.getChildren().size(), 0);
  }
}
