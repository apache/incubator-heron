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
<<<<<<< HEAD
import com.twitter.heron.streamlet.Context
import com.twitter.heron.streamlet.scala.Source
import com.twitter.heron.streamlet.scala.Builder
import com.twitter.heron.streamlet.scala.converter.ScalaToJavaConverter
import com.twitter.heron.streamlet.scala.impl.StreamletImpl
=======
>>>>>>> d76d869b6... more changes for Builder

import scala.collection.mutable.ListBuffer

/**
  * Tests for Scala Builder Implementation functionality
  */
class BuilderImplTest extends BaseFunSuite {
<<<<<<< HEAD



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
=======
  test(
    "BuilderImpl should support generating a streamlet from a user generated source function") {
      val source = MySource

    }


  "BuilderImpl should support generating a streamlet from a user generated serializable supplier function") {
    val source = MySource

  }

  private class MySource() extends Source[Int] {
>>>>>>> d76d869b6... more changes for Builder
    private val numbers = ListBuffer[Int]()

    override def setup(context: Context): Unit = {
      numbers += (1, 2, 3, 4, 5)
    }

    override def get: Iterable[Int] = numbers

    override def cleanup(): Unit = numbers.clear()
  }

}