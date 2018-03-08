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
import com.twitter.heron.streamlet.Context
import com.twitter.heron.streamlet.scala.Source
import com.twitter.heron.streamlet.scala.Builder
import com.twitter.heron.streamlet.scala.converter.ScalaToJavaConverter
import com.twitter.heron.streamlet.scala.impl.StreamletImpl

import scala.collection.mutable.ListBuffer

/**
  * Tests for Scala Builder Implementation functionality
  */
class BuilderImplTest extends BaseFunSuite {


  //val supplierStreamlet = StreamletImpl
  //  .createSupplierStreamlet(() => Math.random)
  //  .setName("Supplier_Streamlet_1")
  //  .setNumPartitions(20)


  //def toSerializableSupplier[T](f: () => T) =
  //  new com.twitter.heron.streamlet.SerializableSupplier[T] {
  //    override def get(): T = f()
  //  }


  /**
    * Create a Streamlet based on the supplier function
    *
    * @param supplier The Supplier function to generate the elements
    *
  private[impl] def createSupplierStreamlet[R](supplier: () => R) = {
    val serializableSupplier = toSerializableSupplier[R](supplier)
    val newJavaStreamlet =
      new com.twitter.heron.streamlet.impl.streamlets.SupplierStreamlet[R](
        serializableSupplier)
    toScalaStreamlet[R](newJavaStreamlet)
  }*/

  test("BuilderImpl should support streamlet generation from a user defined supplier function") {
    val resultOfBuilder = Builder.newBuilder
    val streamletObj = resultOfBuilder.newSource(() => Math.random)
    assert(streamletObj.isInstanceOf[Streamlet[Double]])
  }

  test("BuilderImpl should support streamlet generation from a user defined source function") {
    val source = new MySource
    val resultOfBuilder = Builder.newBuilder
    val streamletObj = resultOfBuilder.newSource(source)
    assert(streamletObj.isInstanceOf[Streamlet[Int]])
  }

  private class MySource extends Source[Int] {
    private val numbers = ListBuffer[Int]()

    override def setup(context: Context): Unit = {
      numbers += (1, 2, 3, 4, 5)
    }

    override def get: Iterable[Int] = numbers

    override def cleanup(): Unit = numbers.clear()
  }

}