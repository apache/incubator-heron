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

import java.util.{Map => JMap}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.junit.Assert.assertEquals

import org.apache.heron.resource.TestSpout
import org.apache.heron.streamlet.Context
import org.apache.heron.streamlet.scala.{Builder, Streamlet, Source}
import org.apache.heron.streamlet.scala.common.BaseFunSuite

/**
  * Tests for Scala Builder Implementation functionality
  */
class BuilderImplTest extends BaseFunSuite {

  test(
    "BuilderImpl should support streamlet generation from a user defined supplier function") {
    val supplierStreamletObj = Builder.newBuilder
      .newSource(() => Math.random)
      .setName("Supplier_Streamlet_1")
      .setNumPartitions(20)
    assert(supplierStreamletObj.isInstanceOf[Streamlet[_]])
    assertEquals("Supplier_Streamlet_1", supplierStreamletObj.getName)
    assertEquals(20, supplierStreamletObj.getNumPartitions)
  }

  test(
    "BuilderImpl should support streamlet generation from a user defined source function") {
    val source = new MySource
    assert(source.get == List[Int]())
    val generatorStreamletObj = Builder.newBuilder
      .newSource(source)
      .setName("Generator_Streamlet_1")
      .setNumPartitions(20)

    assert(generatorStreamletObj.isInstanceOf[Streamlet[_]])
    assertEquals("Generator_Streamlet_1", generatorStreamletObj.getName)
    assertEquals(20, generatorStreamletObj.getNumPartitions)
  }

  private class MySource extends Source[Int] {
    private val numbers = ListBuffer[Int]()

    override def setup(context: Context): Unit = {
      numbers += (1, 2, 3, 4, 5)
    }

    override def get(): Iterable[Int] = numbers

    override def cleanup(): Unit = numbers.clear()
  }

  test(
    "BuilderImpl should support streamlet generation from a user defined spout") {
    val spout = new TestSpout
    val spoutStreamletObj = Builder.newBuilder
      .newSource(spout)
      .setName("Spout_Streamlet_1")
      .setNumPartitions(20)

    assert(spoutStreamletObj.isInstanceOf[Streamlet[_]])
    assertEquals("Spout_Streamlet_1", spoutStreamletObj.getName)
    assertEquals(20, spoutStreamletObj.getNumPartitions)
  }
}
