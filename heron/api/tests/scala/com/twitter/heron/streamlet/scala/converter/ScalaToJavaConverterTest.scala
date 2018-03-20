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

import scala.collection.mutable.ListBuffer

import com.twitter.heron.streamlet.{
  Context,
  SerializableBiFunction,
  SerializableBinaryOperator,
  SerializableConsumer,
  SerializableFunction,
  SerializablePredicate,
  SerializableSupplier,
  SerializableTransformer,
  Sink => JavaSink
}

import com.twitter.heron.streamlet.scala.{Sink, Source}

import com.twitter.heron.streamlet.scala.common.{
  BaseFunSuite,
  TestIncrementSerializableTransformer
}

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
        .isInstanceOf[SerializableSupplier[String]])
  }

  test("ScalaToJavaConverterTest should support SerializableFunction") {
    def stringToIntFunction(number: String) = number.toInt
    val serializableFunction =
      ScalaToJavaConverter.toSerializableFunction[String, Int](
        stringToIntFunction)
    assertTrue(
      serializableFunction
        .isInstanceOf[SerializableFunction[String, Int]])
  }

  test("ScalaToJavaConverterTest should support SerializableBiFunction") {
    def numbersToStringFunction(number1: Int, number2: Long): String =
      (number1.toLong + number2).toString
    val serializableBiFunction =
      ScalaToJavaConverter.toSerializableBiFunction[Int, Long, String](
        numbersToStringFunction)
    assertTrue(
      serializableBiFunction
        .isInstanceOf[SerializableBiFunction[Int, Long, String]])
  }

  test("ScalaToJavaConverterTest should support Java Sink") {
    val javaSink =
      ScalaToJavaConverter.toJavaSink[Int](new TestSink[Int]())
    assertTrue(
      javaSink
        .isInstanceOf[JavaSink[Int]])
  }

  test("ScalaToJavaConverterTest should support Java Source") {
    val javaSource =
      ScalaToJavaConverter.toJavaSource[Int](new TestSource())
    assertTrue(
      javaSource
        .isInstanceOf[com.twitter.heron.streamlet.Source[Int]])
  }

  test("ScalaToJavaConverterTest should support SerializablePredicate") {
    def intToBooleanFunction(number: Int) = number.<(5)
    val serializablePredicate =
      ScalaToJavaConverter.toSerializablePredicate[Int](intToBooleanFunction)
    assertTrue(
      serializablePredicate
        .isInstanceOf[SerializablePredicate[Int]])
  }

  test("ScalaToJavaConverterTest should support SerializableConsumer") {
    def consumerFunction(number: Int): Unit = number * 10
    val serializableConsumer =
      ScalaToJavaConverter.toSerializableConsumer[Int](consumerFunction)
    assertTrue(
      serializableConsumer
        .isInstanceOf[SerializableConsumer[Int]])
  }

  test("ScalaToJavaConverterTest should support SerializableBinaryOperator") {
    def addNumbersFunction(number1: Int, number2: Int): Int =
      number1 + number2
    val serializableBinaryOperator =
      ScalaToJavaConverter.toSerializableBinaryOperator[Int](addNumbersFunction)
    assertTrue(
      serializableBinaryOperator
        .isInstanceOf[SerializableBinaryOperator[Int]])
  }

  test("ScalaToJavaConverterTest should support SerializableTransformer") {
    val serializableTransformer =
      new TestIncrementSerializableTransformer(factor = 100)

    val javaSerializableTransformer =
      ScalaToJavaConverter.toSerializableTransformer[Int, Int](
        serializableTransformer)
    assertTrue(
      javaSerializableTransformer
        .isInstanceOf[SerializableTransformer[Int, Int]])
  }

  private class TestSink[T] extends Sink[T] {
    override def setup(context: Context): Unit = {}
    override def put(tuple: T): Unit = {}
    override def cleanup(): Unit = {}
  }

  private class TestSource() extends Source[Int] {
    private val numbers = ListBuffer[Int]()
    override def setup(context: Context): Unit = {
      numbers += (1, 2, 3, 4, 5)
    }
    override def get(): Iterable[Int] = numbers

    override def cleanup(): Unit = numbers.clear()
  }

}
