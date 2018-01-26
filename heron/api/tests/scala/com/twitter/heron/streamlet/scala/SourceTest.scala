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

import java.{io, util}
import java.util.function.Supplier

import com.twitter.heron.api.state.State
import com.twitter.heron.streamlet.Context
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.WordSpec

import scala.collection.mutable.ListBuffer

@RunWith(classOf[JUnitRunner])
class SourceTest extends WordSpec {

  "SourceTest" should {

    val expectedList = List(1, 2, 3, 4, 5)

    "provide data" in {
      val source = new MySource()
      source.setup(new MyContext())
      assert(source.get == expectedList)
    }

    "support cleanup" in {
      val source = new MySource()
      source.setup(new MyContext())
      assert(source.get == expectedList)
      source.cleanup()
      assert(source.get == List[Int]())
    }

  }

  private class MySource() extends Source[Int] {
    private val numbers = ListBuffer[Int]()

    override def setup(context: Context): Unit = {
      numbers += (1, 2, 3, 4, 5)
    }

    override def get: Iterable[Int] = numbers

    override def cleanup(): Unit = numbers.clear()
  }

  private class MyContext extends Context {
    override def getTaskId: Int = ???

    override def getConfig: util.Map[String, AnyRef] = ???

    override def getStreamName: String = ???

    override def getStreamPartition: Int = ???

    override def registerMetric[T](metricName: String, collectionInterval: Int, metricFn: Supplier[T]): Unit = ???

    override def getState: State[io.Serializable, io.Serializable] = ???
  }
}
