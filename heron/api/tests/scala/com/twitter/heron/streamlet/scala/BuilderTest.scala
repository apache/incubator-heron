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
package com.twitter.heron.streamlet.scala

import scala.collection.mutable.ListBuffer
import com.twitter.heron.streamlet.Context
import com.twitter.heron.streamlet.scala.common.{BaseFunSuite, TestContext}


/**
  * Tests for Builder Trait functionality
  */
class BuilderTest extends BaseFunSuite {
  def testFunc:Unit= {
    println("time flies like an arrow ...")
  }


  /*test("Builder should provide new source based on user defined source function") {
    val builderObj = Builder.newBuilder
    val result:Streamlet[Int]=builderObj.newSource(new BuilderSource)
    assert(result!=null)
  }*/

  test("Builder should provide new source based on user defined supplier function") {
    val builderObj = Builder.newBuilder
    val result =builderObj.newSource(testFunc)
    assert(result!=null)
  }

  /*private class BuilderSource extends Source[Int] {
    private val numbers = ListBuffer[Int]()

    override def setup(context: Context): Unit = {
      numbers += (1, 2, 3, 4, 5)
    }

    override def get: Iterable[Int] = numbers

    override def cleanup(): Unit = numbers.clear()
  }*/




}
