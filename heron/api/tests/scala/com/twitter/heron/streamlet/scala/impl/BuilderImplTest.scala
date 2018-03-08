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

<<<<<<< HEAD

import com.twitter.heron.streamlet.scala.{Builder,Streamlet,Source}
import com.twitter.heron.streamlet.scala.common.BaseFunSuite
import org.junit.Assert.{assertEquals, assertTrue}
import com.twitter.heron.streamlet.Context

=======
import com.twitter.heron.streamlet.scala.Streamlet
import com.twitter.heron.streamlet.scala.common.BaseFunSuite
import org.junit.Assert.{assertEquals, assertTrue}
import com.twitter.heron.streamlet.Context
import com.twitter.heron.streamlet.scala.Source
import com.twitter.heron.streamlet.scala.Builder
import com.twitter.heron.streamlet.scala.converter.ScalaToJavaConverter
import com.twitter.heron.streamlet.scala.impl.StreamletImpl
>>>>>>> ae589f0969c78168a305983f2037a2db9dcd56d4

import scala.collection.mutable.ListBuffer

/**
  * Tests for Scala Builder Implementation functionality
  */
class BuilderImplTest extends BaseFunSuite {


  test("BuilderImpl should support streamlet generation from a user defined supplier function") {
    val supplierStreamletObj = Builder.newBuilder.newSource(() => Math.random).setName("Supplier_Streamlet_1").setNumPartitions(20)
    assert(supplierStreamletObj.isInstanceOf[Streamlet[Int]])
    assertEquals("Supplier_Streamlet_1", supplierStreamletObj.getName)
    assertEquals(20, supplierStreamletObj.getNumPartitions)
  }

  test("BuilderImpl should support streamlet generation from a user defined source function") {
    val source = new MySource
    assert(source.get == List[Int]())
    val generatorStreamletObj = Builder.newBuilder.newSource(source).setName("Generator_Streamlet_1").setNumPartitions(20)

    assert(generatorStreamletObj.isInstanceOf[Streamlet[Int]])
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

}