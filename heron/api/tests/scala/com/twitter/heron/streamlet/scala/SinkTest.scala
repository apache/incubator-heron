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
  * Sink is how Streamlet's end. This class covers unit tests of Sink functionality
  * by creating Test Sink Implementation
  */
class SinkTest extends BaseFunSuite {

  test("Sink should support setup") {
    val list = ListBuffer[Int]()
    val sink = new TestSink(list)
    sink.setup(new TestContext())
    assert(list == List(1, 2))
  }

  test("Sink should put data") {
    val list = ListBuffer[Int]()
    val sink = new TestSink(list)
    sink.setup(new TestContext())
    sink.put(3)
    assert(list == List(1, 2, 3))
  }

  test("Sink should support cleanup") {
    val list = ListBuffer[Int]()
    val sink = new TestSink(list)
    sink.setup(new TestContext())
    sink.put(3)
    sink.cleanup()
    assert(list.isEmpty)
  }

  private class TestSink(numbers: ListBuffer[Int]) extends Sink[Int] {

    override def setup(context: Context): Unit = {
      numbers += (1, 2)
    }

    override def put(tuple: Int): Unit = {
      numbers += tuple
    }

    override def cleanup(): Unit = numbers.clear()

  }

}
