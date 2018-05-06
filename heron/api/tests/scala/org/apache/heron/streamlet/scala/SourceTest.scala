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
package org.apache.heron.streamlet.scala

import scala.collection.mutable.ListBuffer

import org.apache.heron.streamlet.Context
import org.apache.heron.streamlet.scala.common.{BaseFunSuite, TestContext}

/**
  * Tests for Source Trait functionality
  */
class SourceTest extends BaseFunSuite {

  val expectedList = List(1, 2, 3, 4, 5)

  test("Source should provide data") {
    val source = new MySource()
    source.setup(new TestContext())
    assert(source.get == expectedList)
  }

  test("Source should support cleanup") {
    val source = new MySource()
    source.setup(new TestContext())
    assert(source.get == expectedList)
    source.cleanup()
    assert(source.get == List[Int]())
  }

  private class MySource() extends Source[Int] {
    private val numbers = ListBuffer[Int]()

    override def setup(context: Context): Unit = {
      numbers += (1, 2, 3, 4, 5)
    }

    override def get: Iterable[Int] = numbers

    override def cleanup(): Unit = numbers.clear()
  }

}
