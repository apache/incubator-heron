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
package com.twitter.heron.streamlet.scala.converter

import org.junit.Assert.assertTrue

import com.twitter.heron.streamlet.Context
import com.twitter.heron.streamlet.scala.Sink
import com.twitter.heron.streamlet.scala.Source
import com.twitter.heron.streamlet.scala.common.BaseFunSuite

/**
  * Tests for Streamlet APIs' Scala to Java Conversion functionality
  */
class ScalaToJavaConverterTest extends BaseFunSuite {

  test("ScalaToJavaConverterTest should support SerializableSupplier") {
    def testFunction() = ""
    val serializableSupplier =
      ScalaToJavaConverter.toSerializableSupplier[String](testFunction)
    assertTrue(
      serializableSupplier
        .isInstanceOf[com.twitter.heron.streamlet.SerializableSupplier[_]])
  }

  test("ScalaToJavaConverterTest should support SerializableFunction") {
    def stringToIntFunction(number: String) = number.toInt
    val serializableFunction =
      ScalaToJavaConverter.toSerializableFunction[String, Int](
        stringToIntFunction)
    assertTrue(
      serializableFunction
        .isInstanceOf[com.twitter.heron.streamlet.SerializableFunction[_, _]])
  }

  test("ScalaToJavaConverterTest should support Java Sink") {
    val javaSink =
      ScalaToJavaConverter.toJavaSink[Int](new TestSink())
    assertTrue(
      javaSink
        .isInstanceOf[com.twitter.heron.streamlet.Sink[Int]])
  }

  private class TestSink() extends Sink[Int] {
    override def setup(context: Context): Unit = {}
    override def put(tuple: Int): Unit = {}
    override def cleanup(): Unit = {}
  }

}
