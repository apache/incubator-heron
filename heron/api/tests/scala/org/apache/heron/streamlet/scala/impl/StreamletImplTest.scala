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
package org.apache.heron.streamlet.scala.impl

import scala.util.Random

import org.junit.Assert.{assertEquals, assertTrue}

import org.apache.heron.resource.{
  TestBasicBolt,
  TestBolt,
  TestWindowBolt
}
import org.apache.heron.streamlet.{
  IStreamletBasicOperator,
  IStreamletRichOperator,
  IStreamletWindowOperator,
  WindowConfig
}
import org.apache.heron.streamlet.impl.streamlets.{
  ConsumerStreamlet,
  CountByKeyStreamlet,
  CountByKeyAndWindowStreamlet,
  CustomStreamlet,
  FilterStreamlet,
  FlatMapStreamlet,
  GeneralReduceByKeyStreamlet,
  LogStreamlet,
  JoinStreamlet,
  KeyByStreamlet,
  MapStreamlet,
  ReduceByKeyStreamlet,
  ReduceByKeyAndWindowStreamlet,
  RemapStreamlet,
  TransformStreamlet,
  SinkStreamlet,
  SplitStreamlet,
  UnionStreamlet
}

import org.apache.heron.streamlet.scala.{Builder, Streamlet, StreamletReducers}
import org.apache.heron.streamlet.scala.common.{
  BaseFunSuite,
  TestIncrementSerializableTransformer,
  TestListBufferSink
}

/**
  * Tests for Scala Streamlet Implementation functionality
  */
class StreamletImplTest extends BaseFunSuite {

  val builder = Builder.newBuilder

  test(
    "StreamletImpl should support setting name and number of partitions per streamlet") {
    val supplierStreamlet = builder
      .newSource(() => Math.random)
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(20)

    assertTrue(supplierStreamlet.isInstanceOf[Streamlet[Double]])
    assertEquals("Supplier_Streamlet_1", supplierStreamlet.getName)
    assertEquals(20, supplierStreamlet.getNumPartitions)

    val mapStreamlet = supplierStreamlet
      .map[Double] { num: Double =>
        num * 10
      }
      .setName("Map_Streamlet_1")
      .setNumPartitions(5)

    assertTrue(mapStreamlet.isInstanceOf[Streamlet[Double]])
    assertEquals("Map_Streamlet_1", mapStreamlet.getName)
    assertEquals(5, mapStreamlet.getNumPartitions)
  }

  test("StreamletImpl should support map transformation") {
    val supplierStreamlet = builder
      .newSource(() => Math.random)
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(20)

    supplierStreamlet
      .map[String] { num: Double =>
        (num * 10).toString
      }
      .setName("Map_Streamlet_1")
      .setNumPartitions(5)

    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[Double]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[MapStreamlet[_, _]])
    val mapStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[MapStreamlet[Double, String]]
    assertEquals("Map_Streamlet_1", mapStreamlet.getName)
    assertEquals(0, mapStreamlet.getChildren.size())
  }

  test("StreamletImpl should support flatMap transformation") {
    val supplierStreamlet = builder
      .newSource(() => Math.random)
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(20)

    supplierStreamlet
      .flatMap[String] { num: Double =>
        List((num * 10).toString)
      }
      .setName("FlatMap_Streamlet_1")
      .setNumPartitions(5)

    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[Double]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[FlatMapStreamlet[_, _]])
    val flatMapStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[FlatMapStreamlet[Double, String]]
    assertEquals("FlatMap_Streamlet_1", flatMapStreamlet.getName)
    assertEquals(0, flatMapStreamlet.getChildren.size())
  }

  test("StreamletImpl should support filter transformation") {
    val supplierStreamlet = builder
      .newSource(() => Math.random)
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(20)

    supplierStreamlet
      .filter { num: Double =>
        num > 10
      }
      .setName("Filter_Streamlet_1")
      .setNumPartitions(5)

    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[Double]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[FilterStreamlet[_]])
    val filterStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[FilterStreamlet[Double]]
    assertEquals("Filter_Streamlet_1", filterStreamlet.getName)
    assertEquals(0, filterStreamlet.getChildren.size())
  }

  test("StreamletImpl should support repartition transformation") {
    val supplierStreamlet = builder
      .newSource(() => "aa bb cc dd ee")
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(5)

    supplierStreamlet
      .repartition(10)
      .setName("Repartitioned_Streamlet_1")

    assertEquals(5, supplierStreamlet.getNumPartitions)

    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[String]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[MapStreamlet[_, _]])
    val repartitionedStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[MapStreamlet[String, String]]
    assertEquals("Repartitioned_Streamlet_1", repartitionedStreamlet.getName)
    assertEquals(0, repartitionedStreamlet.getChildren.size())
    assertEquals(10, repartitionedStreamlet.getNumPartitions)
  }

  test(
    "StreamletImpl should support repartition transformation with partition function") {
    val supplierStreamlet = builder
      .newSource(() => "aa bb cc dd ee")
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(5)

    def partitionFunction(number1: String, number2: Int): Seq[Int] =
      Seq(number1.toInt + number2)

    supplierStreamlet
      .repartition(10, partitionFunction)
      .setName("Repartitioned_Streamlet_1")

    assertEquals(5, supplierStreamlet.getNumPartitions)

    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[String]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[RemapStreamlet[_]])

    val repartitionedStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[RemapStreamlet[String]]
    assertEquals("Repartitioned_Streamlet_1", repartitionedStreamlet.getName)
    assertEquals(0, repartitionedStreamlet.getChildren.size())
    assertEquals(10, repartitionedStreamlet.getNumPartitions)
  }

  test("StreamletImpl should support union transformation") {
    val supplierStreamlet = builder
      .newSource(() => "aa bb cc dd ee")
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(2)

    val supplierStreamlet2 = builder
      .newSource(() => "fff ggg hhh")
      .setName("Supplier_Streamlet_2")
      .setNumPartitions(3)

    supplierStreamlet
      .union(supplierStreamlet2)
      .setName("Union_Streamlet_1")
      .setNumPartitions(4)

    verifySupplierStreamlet(supplierStreamlet)
    verifySupplierStreamlet(supplierStreamlet2)
  }

  test("StreamletImpl should support split and withStream transformation") {
    val supplierStreamlet = builder
      .newSource(() => Math.random)
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(20)

    val splitted = supplierStreamlet
      .split(Map(
        "positive" -> { num: Double => num > 0 },
        "negative" -> { num: Double => num < 0 }
      ))
      .setName("Split_Streamlet_1")
      .setNumPartitions(5)

    splitted.withStream("positive")
      .map { num: Double =>
        num * 10
      }

    splitted.withStream("negative")
      .map { num: Double =>
        num * -10
      }

    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[Double]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[SplitStreamlet[_]])

    val splitStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[SplitStreamlet[Double]]
    assertEquals("Split_Streamlet_1", splitStreamlet.getName)
    assertEquals(2, splitStreamlet.getChildren.size())
    assertTrue(
      splitStreamlet
        .getChildren.get(0)
        .isInstanceOf[MapStreamlet[_, _]])
    assertTrue(
      splitStreamlet
        .getChildren.get(1)
        .isInstanceOf[MapStreamlet[_, _]])
    
    val mapStreamlet1 = splitStreamlet
      .getChildren.get(0)
      .asInstanceOf[MapStreamlet[Double, Double]]
    assertEquals("positive", mapStreamlet1.getParent.getStreamId)
    val mapStreamlet2 = splitStreamlet
      .getChildren.get(1)
      .asInstanceOf[MapStreamlet[Double, Double]]
    assertEquals("negative", mapStreamlet2.getParent.getStreamId)
  }

  test("StreamletImpl should support consume function") {
    val supplierStreamlet = builder
      .newSource(() => Math.random)
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(20)

    supplierStreamlet
      .consume { num: Double =>
        num > 10
      }

    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[Double]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[ConsumerStreamlet[_]])
    val consumerStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[ConsumerStreamlet[Double]]
    assertEquals(null, consumerStreamlet.getName)
    assertEquals(0, consumerStreamlet.getChildren.size())
    assertEquals(20, consumerStreamlet.getNumPartitions)
  }

  test("StreamletImpl should support log sink") {
    val supplierStreamlet = builder
      .newSource(() => Math.random)
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(10)

    supplierStreamlet
      .log()

    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[Double]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[LogStreamlet[_]])
    val consumerStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[LogStreamlet[Double]]
    assertEquals(null, consumerStreamlet.getName)
    assertEquals(0, consumerStreamlet.getChildren.size())
    assertEquals(10, consumerStreamlet.getNumPartitions)
  }

  test("StreamletImpl should support custom sink") {
    val supplierStreamlet = builder
      .newSource(() => Random.nextInt(10))
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(10)

    supplierStreamlet
      .toSink(new TestListBufferSink())

    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[Int]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[SinkStreamlet[_]])
    val consumerStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[SinkStreamlet[Int]]
    assertEquals(null, consumerStreamlet.getName)
    assertEquals(0, consumerStreamlet.getChildren.size())
    assertEquals(10, consumerStreamlet.getNumPartitions)
  }

  test("StreamletImpl should support join transformation") {
    val numberStreamlet = builder
      .newSource(() => Random.nextInt(10))
      .setName("Supplier_Streamlet_with_Numbers")
      .setNumPartitions(4)

    val textStreamlet = builder
      .newSource(() => Random.nextString(3))
      .setName("Supplier_Streamlet_with_Strings")
      .setNumPartitions(3)

    numberStreamlet
      .join[String, String, String](textStreamlet,
                                    (x: Int) => x.toString,
                                    (y: String) => y,
                                    WindowConfig.TumblingCountWindow(10),
                                    (x: Int, y: String) => x + y)
      .setName("Joined_Streamlet_1")
      .setNumPartitions(2)

    verifyJoinedStreamlet[Int](numberStreamlet,
                               expectedName = "Joined_Streamlet_1",
                               expectedNumPartitions = 2)
    verifyJoinedStreamlet[String](textStreamlet,
                                  expectedName = "Joined_Streamlet_1",
                                  expectedNumPartitions = 2)
  }

  test("StreamletImpl should support clone operation") {
    val supplierStreamlet = builder
      .newSource(() => Math.random)
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(5)

    val clonedStreamlets = supplierStreamlet.clone(numClones = 3)
    assertEquals(3, clonedStreamlets.size)

    verifyClonedStreamlets[Double](supplierStreamlet, numClones = 3)
  }

  test("StreamletImpl should support transform operation") {
    val incrementTransformer =
      new TestIncrementSerializableTransformer(factor = 100)
    val supplierStreamlet = builder
      .newSource(() => Random.nextInt(10))
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(3)

    supplierStreamlet
      .map[Int] { num: Int =>
        num * 10
      }
      .setName("Map_Streamlet_1")
      .setNumPartitions(2)
      .transform[Int](incrementTransformer)
      .setName("Transformer_Streamlet_1")
      .setNumPartitions(7)

    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[Int]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[MapStreamlet[_, _]])
    val mapStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[MapStreamlet[Int, Int]]
    assertEquals("Map_Streamlet_1", mapStreamlet.getName)
    assertEquals(2, mapStreamlet.getNumPartitions)
    assertEquals(1, mapStreamlet.getChildren.size())

    assertTrue(
      mapStreamlet
        .getChildren()
        .get(0)
        .isInstanceOf[TransformStreamlet[_, _]])
    val transformStreamlet = mapStreamlet
      .getChildren()
      .get(0)
      .asInstanceOf[TransformStreamlet[Int, Int]]
    assertEquals("Transformer_Streamlet_1", transformStreamlet.getName)
    assertEquals(7, transformStreamlet.getNumPartitions)
    assertEquals(0, transformStreamlet.getChildren.size())
  }

  private class MyBoltOperator extends TestBolt with IStreamletRichOperator[Double, Double] {
  }

  test("StreamletImpl should support applyOperator operation on IStreamletRichOperator") {
    
    val testOperator = new MyBoltOperator()
    val supplierStreamlet = builder
      .newSource(() => Random.nextDouble())
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(3)

    supplierStreamlet
      .map[Double] { num: Double =>
        num * 10
      }
      .setName("Map_Streamlet_1")
      .setNumPartitions(2)
      .applyOperator(testOperator)
      .setName("Custom_Streamlet_1")
      .setNumPartitions(7)

    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[Double]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[MapStreamlet[_, _]])
    val mapStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[MapStreamlet[Double, Double]]
    assertEquals("Map_Streamlet_1", mapStreamlet.getName)
    assertEquals(2, mapStreamlet.getNumPartitions)
    assertEquals(1, mapStreamlet.getChildren.size())

    assertTrue(
      mapStreamlet
        .getChildren()
        .get(0)
        .isInstanceOf[CustomStreamlet[_, _]])
    val customStreamlet = mapStreamlet
      .getChildren()
      .get(0)
      .asInstanceOf[CustomStreamlet[Double, Double]]
    assertEquals("Custom_Streamlet_1", customStreamlet.getName)
    assertEquals(7, customStreamlet.getNumPartitions)
    assertEquals(0, customStreamlet.getChildren.size())
  }

  private class MyBasicBoltOperator extends TestBasicBolt
      with IStreamletBasicOperator[Double, Double] {
  }

  test("StreamletImpl should support applyOperator operation on IStreamletBasicOperator") {
    val testOperator = new MyBasicBoltOperator()
    val supplierStreamlet = builder
      .newSource(() => Random.nextDouble())
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(3)

    supplierStreamlet
      .map[Double] { num: Double =>
        num * 10
      }
      .setName("Map_Streamlet_1")
      .setNumPartitions(2)
      .applyOperator(testOperator)
      .setName("CustomBasic_Streamlet_1")
      .setNumPartitions(7)

    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[Double]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[MapStreamlet[_, _]])
    val mapStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[MapStreamlet[Double, Double]]
    assertEquals("Map_Streamlet_1", mapStreamlet.getName)
    assertEquals(2, mapStreamlet.getNumPartitions)
    assertEquals(1, mapStreamlet.getChildren.size())

    assertTrue(
      mapStreamlet
        .getChildren()
        .get(0)
        .isInstanceOf[CustomStreamlet[_, _]])
    val customStreamlet = mapStreamlet
      .getChildren()
      .get(0)
      .asInstanceOf[CustomStreamlet[Double, Double]]
    assertEquals("CustomBasic_Streamlet_1", customStreamlet.getName)
    assertEquals(7, customStreamlet.getNumPartitions)
    assertEquals(0, customStreamlet.getChildren.size())
  }

  private class MyWindowBoltOperator extends TestWindowBolt
      with IStreamletWindowOperator[Double, Double] {
  }

  test("StreamletImpl should support applyOperator operation on IStreamletWindowOperator") {
    val testOperator = new MyWindowBoltOperator()
    val supplierStreamlet = builder
      .newSource(() => Random.nextDouble())
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(3)

    supplierStreamlet
      .map[Double] { num: Double =>
        num * 10
      }
      .setName("Map_Streamlet_1")
      .setNumPartitions(2)
      .applyOperator(testOperator)
      .setName("CustomWindow_Streamlet_1")
      .setNumPartitions(7)

    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[Double]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[MapStreamlet[_, _]])
    val mapStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[MapStreamlet[Double, Double]]
    assertEquals("Map_Streamlet_1", mapStreamlet.getName)
    assertEquals(2, mapStreamlet.getNumPartitions)
    assertEquals(1, mapStreamlet.getChildren.size())

    assertTrue(
      mapStreamlet
        .getChildren()
        .get(0)
        .isInstanceOf[CustomStreamlet[_, _]])
    val customStreamlet = mapStreamlet
      .getChildren()
      .get(0)
      .asInstanceOf[CustomStreamlet[Double, Double]]
    assertEquals("CustomWindow_Streamlet_1", customStreamlet.getName)
    assertEquals(7, customStreamlet.getNumPartitions)
    assertEquals(0, customStreamlet.getChildren.size())
  }

  test("StreamletImpl should support reduce by key operation") {
    val supplierStreamlet = builder
      .newSource(() => Random.nextInt(10))
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(3)

    supplierStreamlet
      .reduceByKey[Int, Int]((x: Int) => x * 100,
                             (x: Int) => x,
                             StreamletReducers.sum(_: Int, _: Int))
      .setName("Reduce_Streamlet_1")
      .setNumPartitions(5)

    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[Int]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[ReduceByKeyStreamlet[_, _, _]])
    val reduceStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[ReduceByKeyStreamlet[Int, Int, Int]]
    assertEquals("Reduce_Streamlet_1", reduceStreamlet.getName)
    assertEquals(5, reduceStreamlet.getNumPartitions)
    assertEquals(0, reduceStreamlet.getChildren.size())
  }

  test("StreamletImpl should support general reduce by key operation") {
    val supplierStreamlet = builder
      .newSource(() => Random.nextInt(10))
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(3)

    supplierStreamlet
      .reduceByKey[Int, Int]((key: Int) => key * 100,
                             0,
                             StreamletReducers.sum(_: Int, _: Int))
      .setName("Reduce_Streamlet_1")
      .setNumPartitions(5)

    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[Int]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[GeneralReduceByKeyStreamlet[_, _, _]])
    val reduceStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[GeneralReduceByKeyStreamlet[Int, Int, Int]]
    assertEquals("Reduce_Streamlet_1", reduceStreamlet.getName)
    assertEquals(5, reduceStreamlet.getNumPartitions)
    assertEquals(0, reduceStreamlet.getChildren.size())
  }

  test("StreamletImpl should support reduce by key and window operation") {
    val supplierStreamlet = builder
      .newSource(() => Random.nextInt(10))
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(3)

    supplierStreamlet
      .reduceByKeyAndWindow[Int, Int]((key: Int) => key * 100,
                                      (value: Int) => 1,
                                      WindowConfig.TumblingCountWindow(10),
                                      StreamletReducers.sum(_: Int, _: Int))
      .setName("Reduce_Streamlet_1")
      .setNumPartitions(5)

    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[Int]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[ReduceByKeyAndWindowStreamlet[_, _, _]])
    val reduceStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[ReduceByKeyAndWindowStreamlet[Int, Int, Int]]
    assertEquals("Reduce_Streamlet_1", reduceStreamlet.getName)
    assertEquals(5, reduceStreamlet.getNumPartitions)
    assertEquals(0, reduceStreamlet.getChildren.size())
  }

  test("StreamletImpl should support count by key operation") {
    val supplierStreamlet = builder
      .newSource(() => Random.nextInt(10))
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(3)

    supplierStreamlet
      .countByKey[Int]((x: Int) => x * 100)
      .setName("Count_Streamlet_1")
      .setNumPartitions(5)

    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[Int]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[CountByKeyStreamlet[_, _]])
    val countStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[CountByKeyStreamlet[Int, Int]]
    assertEquals("Count_Streamlet_1", countStreamlet.getName)
    assertEquals(5, countStreamlet.getNumPartitions)
    assertEquals(0, countStreamlet.getChildren.size())
  }

  test("StreamletImpl should support count by key and window operation") {
    val supplierStreamlet = builder
      .newSource(() => Random.nextInt(10))
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(3)

    supplierStreamlet
      .countByKeyAndWindow[Int]((x: Int) => x * 100,
                                WindowConfig.TumblingCountWindow(10))
      .setName("Count_Streamlet_1")
      .setNumPartitions(5)

    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[Int]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[CountByKeyAndWindowStreamlet[_, _]])
    val countStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[CountByKeyAndWindowStreamlet[Int, Int]]
    assertEquals("Count_Streamlet_1", countStreamlet.getName)
    assertEquals(5, countStreamlet.getNumPartitions)
    assertEquals(0, countStreamlet.getChildren.size())
  }

  test("StreamletImpl should support keyBy operation") {
    val supplierStreamlet = builder
      .newSource(() => Random.nextInt(10))
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(3)

    supplierStreamlet
      .keyBy[Int, Int]((key: Int) => key % 3,  // Put into 3 groups
                       (value: Int) => value)
      .setName("KeyBy_Streamlet_1")
      .setNumPartitions(5)

    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[Int]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[KeyByStreamlet[_, _, _]])
    val keyByStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[KeyByStreamlet[Int, Int, Int]]
    assertEquals("KeyBy_Streamlet_1", keyByStreamlet.getName)
    assertEquals(5, keyByStreamlet.getNumPartitions)
    assertEquals(0, keyByStreamlet.getChildren.size())
  }

  private def verifyClonedStreamlets[R](supplierStreamlet: Streamlet[R],
                                        numClones: Int): Unit = {
    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[R]]
    assertEquals(numClones, supplierStreamletImpl.getChildren.size)
    for (index <- 0 to (numClones - 1)) {
      assertTrue(
        supplierStreamletImpl
          .getChildren(index)
          .isInstanceOf[MapStreamlet[_, _]])
      val mapStreamlet = supplierStreamletImpl
        .getChildren(index)
        .asInstanceOf[MapStreamlet[R, R]]
      assertEquals(0, mapStreamlet.getChildren.size())
    }
  }

  private def verifyJoinedStreamlet[R](supplierStreamlet: Streamlet[R],
                                       expectedName: String,
                                       expectedNumPartitions: Int): Unit = {
    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[R]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[JoinStreamlet[_, _, _, _]])
    val joinStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[JoinStreamlet[String, Int, String, String]]
    assertEquals(expectedName, joinStreamlet.getName)
    assertEquals(expectedNumPartitions, joinStreamlet.getNumPartitions)
    assertEquals(0, joinStreamlet.getChildren.size())
  }

  private def verifySupplierStreamlet(
      supplierStreamlet: Streamlet[String]): Unit = {
    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[String]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[UnionStreamlet[_]])
    val unionStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[UnionStreamlet[String]]
    assertEquals("Union_Streamlet_1", unionStreamlet.getName)
    assertEquals(0, unionStreamlet.getChildren.size())
    assertEquals(4, unionStreamlet.getNumPartitions)
  }

}
