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

import org.apache.heron.streamlet.WindowConfig
import org.apache.heron.streamlet.impl.streamlets.{
  ConsumerStreamlet,
  FilterStreamlet,
  FlatMapStreamlet,
  LogStreamlet,
  JoinStreamlet,
  MapStreamlet,
  ReduceByKeyAndWindowStreamlet,
  RemapStreamlet,
  TransformStreamlet,
  SinkStreamlet,
  UnionStreamlet
}

import org.apache.heron.streamlet.scala.Streamlet
import org.apache.heron.streamlet.scala.common.{
  BaseFunSuite,
  TestIncrementSerializableTransformer,
  TestListBufferSink
}

/**
  * Tests for Scala Streamlet Implementation functionality
  */
class StreamletImplTest extends BaseFunSuite {

  test(
    "StreamletImpl should support setting name and number of partitions per streamlet") {
    val supplierStreamlet = StreamletImpl
      .createSupplierStreamlet(() => Math.random)
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
    val supplierStreamlet = StreamletImpl
      .createSupplierStreamlet(() => Math.random)
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
    val supplierStreamlet = StreamletImpl
      .createSupplierStreamlet(() => Math.random)
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
    val supplierStreamlet = StreamletImpl
      .createSupplierStreamlet(() => Math.random)
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
    val supplierStreamlet = StreamletImpl
      .createSupplierStreamlet(() => "aa bb cc dd ee")
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
    val supplierStreamlet = StreamletImpl
      .createSupplierStreamlet(() => "aa bb cc dd ee")
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
    val supplierStreamlet = StreamletImpl
      .createSupplierStreamlet(() => "aa bb cc dd ee")
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(2)

    val supplierStreamlet2 = StreamletImpl
      .createSupplierStreamlet(() => "fff ggg hhh")
      .setName("Supplier_Streamlet_2")
      .setNumPartitions(3)

    supplierStreamlet
      .union(supplierStreamlet2)
      .setName("Union_Streamlet_1")
      .setNumPartitions(4)

    verifySupplierStreamlet(supplierStreamlet)
    verifySupplierStreamlet(supplierStreamlet2)
  }

  test("StreamletImpl should support consume function") {
    val supplierStreamlet = StreamletImpl
      .createSupplierStreamlet(() => Math.random)
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
    val supplierStreamlet = StreamletImpl
      .createSupplierStreamlet(() => Math.random)
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
    val supplierStreamlet = StreamletImpl
      .createSupplierStreamlet(() => Random.nextInt(10))
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
    val numberStreamlet = StreamletImpl
      .createSupplierStreamlet(() => Random.nextInt(10))
      .setName("Supplier_Streamlet_with_Numbers")
      .setNumPartitions(4)

    val textStreamlet = StreamletImpl
      .createSupplierStreamlet(() => Random.nextString(3))
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
    val supplierStreamlet = StreamletImpl
      .createSupplierStreamlet(() => Math.random)
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(5)

    val clonedStreamlets = supplierStreamlet.clone(numClones = 3)
    assertEquals(3, clonedStreamlets.size)

    verifyClonedStreamlets[Double](supplierStreamlet, numClones = 3)
  }

  test("StreamletImpl should support transform operation") {
    val incrementTransformer =
      new TestIncrementSerializableTransformer(factor = 100)
    val supplierStreamlet = StreamletImpl
      .createSupplierStreamlet(() => Random.nextInt(10))
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

  test("StreamletImpl should support reduce operation") {
    val supplierStreamlet = StreamletImpl
      .createSupplierStreamlet(() => Random.nextInt(10))
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(3)

    supplierStreamlet
      .reduceByKeyAndWindow[Int, Int]((key: Int) => key * 100,
                                      (value: Int) => 1,
                                      WindowConfig.TumblingCountWindow(10),
                                      (x: Int, y: Int) => x + y)
      .setName("Reduce_Streamlet_1")
      .setNumPartitions(5)

    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[Int]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[ReduceByKeyAndWindowStreamlet[_, _, _]])
    val mapStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[ReduceByKeyAndWindowStreamlet[Int, Int, Int]]
    assertEquals("Reduce_Streamlet_1", mapStreamlet.getName)
    assertEquals(5, mapStreamlet.getNumPartitions)
    assertEquals(0, mapStreamlet.getChildren.size())
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
