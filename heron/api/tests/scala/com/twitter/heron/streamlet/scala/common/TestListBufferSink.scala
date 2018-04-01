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
package com.twitter.heron.streamlet.scala.common

import scala.collection.mutable.ListBuffer

import com.twitter.heron.streamlet.Context
import com.twitter.heron.streamlet.scala.Sink

/**
  * Test ListBuffer Sink for Scala Streamlet API Unit Tests' general usage.
  */
private[scala] class TestListBufferSink(
    numbers: ListBuffer[Int] = ListBuffer[Int]())
    extends Sink[Int] {
  override def setup(context: Context): Unit = numbers += (1, 2)
  override def put(tuple: Int): Unit = numbers += tuple
  override def cleanup(): Unit = numbers.clear()
}
