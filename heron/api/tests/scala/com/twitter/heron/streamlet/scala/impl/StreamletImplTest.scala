//  Copyright 2018 Twitter. All rights reserved.
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
package com.twitter.heron.streamlet.scala.impl

import com.twitter.heron.streamlet.scala.Streamlet
import com.twitter.heron.streamlet.scala.common.BaseFunSuite
import org.junit.Assert.{assertEquals, assertTrue}

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
      .map[Double]((num: Double) => num * 10)
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
      .map[Double]((num: Double) => num * 10)
      .setName("Map_Streamlet_1")
      .setNumPartitions(5)

    val supplierStreamletImpl =
      supplierStreamlet.asInstanceOf[StreamletImpl[Double]]
    assertEquals(1, supplierStreamletImpl.getChildren.size)
    assertTrue(
      supplierStreamletImpl
        .getChildren(0)
        .isInstanceOf[
          com.twitter.heron.streamlet.impl.streamlets.MapStreamlet[_, _]])
    val mapStreamlet = supplierStreamletImpl
      .getChildren(0)
      .asInstanceOf[
        com.twitter.heron.streamlet.impl.streamlets.MapStreamlet[_, _]]
    assertEquals("Map_Streamlet_1", mapStreamlet.getName)
    assertEquals(0, mapStreamlet.getChildren.size())
  }

}
