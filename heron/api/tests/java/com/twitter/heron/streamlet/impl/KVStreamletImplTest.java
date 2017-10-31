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

import org.junit.Before;
import org.junit.Test;


import com.twitter.heron.streamlet.JoinType;
import com.twitter.heron.streamlet.KVStreamlet;
import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.KeyedWindow;
import com.twitter.heron.streamlet.Streamlet;
import com.twitter.heron.streamlet.WindowConfig;
import com.twitter.heron.streamlet.impl.streamlets.GeneralReduceByKeyAndWindowStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.JoinStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.MapToKVStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.SupplierStreamlet;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link StreamletImpl}
 */
public class KVStreamletImplTest {

  private <T> boolean isFullyBuilt(BaseStreamletImpl<T> streamlet) {
    if (!streamlet.isBuilt()) {
      return false;
    }
    for (BaseStreamletImpl<?> child : streamlet.getChildren()) {
      if (!isFullyBuilt(child)) {
        return false;
      }
    }
    return true;
  }

  @Before
  public void setUp() {
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testJoinStreamlet() throws Exception {
    Streamlet<Double> baseStreamlet1 = StreamletImpl.createSupplierStreamlet(() -> Math.random());
    KVStreamlet<Double, Integer> leftStreamlet =
        baseStreamlet1.mapToKV(x -> new KeyValue<>(x, 1));
    assertTrue(leftStreamlet instanceof MapToKVStreamlet);

    Streamlet<Double> baseStreamlet2 = StreamletImpl.createSupplierStreamlet(() -> Math.random());
    KVStreamlet<Double, Integer> rightStreamlet =
        baseStreamlet2.mapToKV(x -> new KeyValue<>(x, 1));
    assertTrue(rightStreamlet instanceof MapToKVStreamlet);

    KVStreamlet<KeyedWindow<Double>, Integer> joinedStreamlet =
        leftStreamlet.join(rightStreamlet, WindowConfig.TumblingCountWindow(10), (x, y) -> x + y);
    assertTrue(joinedStreamlet instanceof JoinStreamlet);
    assertEquals(((JoinStreamlet<Double, Integer, Integer, Integer>) joinedStreamlet)
        .getJoinType(), JoinType.INNER);

    SupplierStreamlet<Double> supplierStreamlet1 = (SupplierStreamlet<Double>) baseStreamlet1;
    assertEquals(supplierStreamlet1.getChildren().size(), 1);
    assertEquals(supplierStreamlet1.getChildren().get(0), leftStreamlet);
    assertEquals(((MapToKVStreamlet<Double, Double, Integer>) leftStreamlet).getChildren().size(),
        1);
    assertTrue(((MapToKVStreamlet<Double, Double, Integer>) leftStreamlet).getChildren().get(0)
        instanceof JoinStreamlet);

    SupplierStreamlet<Double> supplierStreamlet2 = (SupplierStreamlet<Double>) baseStreamlet2;
    assertEquals(supplierStreamlet2.getChildren().size(), 1);
    assertEquals(supplierStreamlet2.getChildren().get(0), rightStreamlet);
    assertEquals(((MapToKVStreamlet<Double, Double, Integer>) rightStreamlet)
            .getChildren().size(),
        1);
    assertTrue(((MapToKVStreamlet<Double, Double, Integer>) rightStreamlet).getChildren().get(0)
        instanceof JoinStreamlet);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLeftJoinStreamlet() throws Exception {
    Streamlet<Double> baseStreamlet1 = StreamletImpl.createSupplierStreamlet(() -> Math.random());
    KVStreamlet<Double, Integer> leftStreamlet =
        baseStreamlet1.mapToKV(x -> new KeyValue<>(x, 1));
    assertTrue(leftStreamlet instanceof MapToKVStreamlet);

    Streamlet<Double> baseStreamlet2 = StreamletImpl.createSupplierStreamlet(() -> Math.random());
    KVStreamlet<Double, Integer> rightStreamlet =
        baseStreamlet2.mapToKV(x -> new KeyValue<>(x, 1));
    assertTrue(rightStreamlet instanceof MapToKVStreamlet);

    KVStreamlet<KeyedWindow<Double>, Integer> joinedStreamlet =
        leftStreamlet.join(rightStreamlet,
            WindowConfig.TumblingCountWindow(10),
            JoinType.OUTER_LEFT, (x, y) -> x + y);
    assertTrue(joinedStreamlet instanceof JoinStreamlet);
    assertEquals(((JoinStreamlet<Double, Integer, Integer, Integer>) joinedStreamlet)
        .getJoinType(), JoinType.OUTER_LEFT);

    SupplierStreamlet<Double> supplierStreamlet1 = (SupplierStreamlet<Double>) baseStreamlet1;
    assertEquals(supplierStreamlet1.getChildren().size(), 1);
    assertEquals(supplierStreamlet1.getChildren().get(0), leftStreamlet);
    assertEquals(((MapToKVStreamlet<Double, Double, Integer>) leftStreamlet).getChildren().size(),
        1);
    assertTrue(((MapToKVStreamlet<Double, Double, Integer>) leftStreamlet).getChildren().get(0)
        instanceof JoinStreamlet);

    SupplierStreamlet<Double> supplierStreamlet2 = (SupplierStreamlet<Double>) baseStreamlet2;
    assertEquals(supplierStreamlet2.getChildren().size(), 1);
    assertEquals(supplierStreamlet2.getChildren().get(0), rightStreamlet);
    assertEquals(((MapToKVStreamlet<Double, Double, Integer>) rightStreamlet).getChildren().size(),
        1);
    assertTrue(((MapToKVStreamlet<Double, Double, Integer>) rightStreamlet).getChildren().get(0)
        instanceof JoinStreamlet);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRightJoinStreamlet() throws Exception {
    Streamlet<Double> baseStreamlet1 = StreamletImpl.createSupplierStreamlet(() -> Math.random());
    KVStreamlet<Double, Integer> leftStreamlet =
        baseStreamlet1.mapToKV(x -> new KeyValue<>(x, 1));
    assertTrue(leftStreamlet instanceof MapToKVStreamlet);

    Streamlet<Double> baseStreamlet2 = StreamletImpl.createSupplierStreamlet(() -> Math.random());
    KVStreamlet<Double, Integer> rightStreamlet =
        baseStreamlet2.mapToKV(x -> new KeyValue<>(x, 1));
    assertTrue(rightStreamlet instanceof MapToKVStreamlet);

    KVStreamlet<KeyedWindow<Double>, Integer> joinedStreamlet =
        leftStreamlet.join(rightStreamlet,
            WindowConfig.TumblingCountWindow(10),
            JoinType.OUTER_RIGHT, (x, y) -> x + y);
    assertTrue(joinedStreamlet instanceof JoinStreamlet);
    assertEquals(((JoinStreamlet<Double, Integer, Integer, Integer>) joinedStreamlet)
        .getJoinType(), JoinType.OUTER_RIGHT);

    SupplierStreamlet<Double> supplierStreamlet1 = (SupplierStreamlet<Double>) baseStreamlet1;
    assertEquals(supplierStreamlet1.getChildren().size(), 1);
    assertEquals(supplierStreamlet1.getChildren().get(0), leftStreamlet);
    assertEquals(((MapToKVStreamlet<Double, Double, Integer>) leftStreamlet).getChildren().size(),
        1);
    assertTrue(((MapToKVStreamlet<Double, Double, Integer>) leftStreamlet).getChildren().get(0)
        instanceof JoinStreamlet);

    SupplierStreamlet<Double> supplierStreamlet2 = (SupplierStreamlet<Double>) baseStreamlet2;
    assertEquals(supplierStreamlet2.getChildren().size(), 1);
    assertEquals(supplierStreamlet2.getChildren().get(0), rightStreamlet);
    assertEquals(((MapToKVStreamlet<Double, Double, Integer>) rightStreamlet).getChildren().size(),
        1);
    assertTrue(((MapToKVStreamlet<Double, Double, Integer>) rightStreamlet).getChildren().get(0)
        instanceof JoinStreamlet);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testOuterJoinStreamlet() throws Exception {
    Streamlet<Double> baseStreamlet1 = StreamletImpl.createSupplierStreamlet(() -> Math.random());
    KVStreamlet<Double, Integer> leftStreamlet =
        baseStreamlet1.mapToKV(x -> new KeyValue<>(x, 1));
    assertTrue(leftStreamlet instanceof MapToKVStreamlet);

    Streamlet<Double> baseStreamlet2 = StreamletImpl.createSupplierStreamlet(() -> Math.random());
    KVStreamlet<Double, Integer> rightStreamlet =
        baseStreamlet2.mapToKV(x -> new KeyValue<>(x, 1));
    assertTrue(rightStreamlet instanceof MapToKVStreamlet);

    KVStreamlet<KeyedWindow<Double>, Integer> joinedStreamlet =
        leftStreamlet.join(rightStreamlet,
            WindowConfig.TumblingCountWindow(10),
            JoinType.OUTER, (x, y) -> x + y);
    assertTrue(joinedStreamlet instanceof JoinStreamlet);
    assertEquals(((JoinStreamlet<Double, Integer, Integer, Integer>) joinedStreamlet)
        .getJoinType(), JoinType.OUTER);

    SupplierStreamlet<Double> supplierStreamlet1 = (SupplierStreamlet<Double>) baseStreamlet1;
    assertEquals(supplierStreamlet1.getChildren().size(), 1);
    assertEquals(supplierStreamlet1.getChildren().get(0), leftStreamlet);
    assertEquals(((MapToKVStreamlet<Double, Double, Integer>) leftStreamlet).getChildren().size(),
        1);
    assertTrue(((MapToKVStreamlet<Double, Double, Integer>) leftStreamlet).getChildren().get(0)
        instanceof JoinStreamlet);

    SupplierStreamlet<Double> supplierStreamlet2 = (SupplierStreamlet<Double>) baseStreamlet2;
    assertEquals(supplierStreamlet2.getChildren().size(), 1);
    assertEquals(supplierStreamlet2.getChildren().get(0), rightStreamlet);
    assertEquals(((MapToKVStreamlet<Double, Double, Integer>) rightStreamlet).getChildren().size(),
        1);
    assertTrue(((MapToKVStreamlet<Double, Double, Integer>) rightStreamlet).getChildren().get(0)
        instanceof JoinStreamlet);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testReduceByKeyAndWindowStreamlet() throws Exception {
    Streamlet<Double> baseStreamlet = StreamletImpl.createSupplierStreamlet(() -> Math.random());
    KVStreamlet<Double, Integer> streamlet =
        baseStreamlet.mapToKV(x -> new KeyValue<>(x, 1));
    assertTrue(streamlet instanceof MapToKVStreamlet);

    KVStreamlet<KeyedWindow<Double>, Integer> rStreamlet =
        streamlet.reduceByKeyAndWindow(WindowConfig.TumblingCountWindow(10), (x, y) -> x + y);
    assertTrue(rStreamlet instanceof GeneralReduceByKeyAndWindowStreamlet);


    SupplierStreamlet<Double> supplierStreamlet = (SupplierStreamlet<Double>) baseStreamlet;
    assertEquals(supplierStreamlet.getChildren().size(), 1);
    assertEquals(supplierStreamlet.getChildren().get(0), streamlet);
    assertEquals(((MapToKVStreamlet<Double, Double, Integer>) streamlet).getChildren().size(),
        1);
    assertTrue(((MapToKVStreamlet<Double, Double, Integer>) streamlet).getChildren().get(0)
        instanceof GeneralReduceByKeyAndWindowStreamlet);
  }
}
