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
package org.apache.heron.streamlet.scala.converter

import java.lang.{Iterable => JavaIterable}
import java.util.{List => JavaList}
import java.util.Collection
import java.util.function.Consumer

import scala.collection.JavaConverters

import org.apache.heron.streamlet.{
  Context,
  SerializableBiFunction,
  SerializableBinaryOperator,
  SerializableConsumer,
  SerializableFunction,
  SerializablePredicate,
  SerializableSupplier,
  SerializableTransformer => JavaSerializableTransformer,
  Sink => JavaSink,
  Source => JavaSource
}

import org.apache.heron.streamlet.scala.{SerializableTransformer, Sink, Source}

/**
  * This class transforms passed User defined Scala Functions, Sources, Sinks
  * to related Java versions
  */
object ScalaToJavaConverter {

  def toSerializableSupplier[T](f: () => T) =
    new SerializableSupplier[T] {
      override def get(): T = f()
    }

  def toSerializableFunction[R, T](f: R => T) =
    new SerializableFunction[R, T] {
      override def apply(r: R): T = f(r)
    }

  def toSerializableFunctionWithIterable[R, T](f: R => Iterable[_ <: T]) =
    new SerializableFunction[R, JavaIterable[_ <: T]] {
      override def apply(r: R): JavaIterable[_ <: T] =
        JavaConverters.asJavaIterableConverter(f(r)).asJava
    }

  def toSerializablePredicate[R](f: R => Boolean) =
    new SerializablePredicate[R] {
      override def test(r: R): Boolean = f(r)
    }

  def toSerializableConsumer[R](f: R => Unit) =
    new SerializableConsumer[R] {
      override def accept(r: R): Unit = f(r)
    }

  def toSerializableBiFunction[R, S, T](f: (R, S) => T) =
    new SerializableBiFunction[R, S, T] {
      override def apply(r: R, s: S): T = f(r, s)
    }

  def toSerializableBinaryOperator[T](f: (T, T) => T) =
    new SerializableBinaryOperator[T] {
      override def apply(t1: T, t2: T): T = f(t1, t2)
    }

  def toSerializableBiFunctionWithSeq[R](f: (R, Int) => Seq[Int]) =
    new SerializableBiFunction[R, Integer, JavaList[Integer]] {
      override def apply(r: R, s: Integer): JavaList[Integer] = {
        val result = f(r, s.intValue()).map(x => Integer.valueOf(x))
        JavaConverters
          .seqAsJavaListConverter[Integer](result)
          .asJava
      }
    }

  def toJavaSink[T](sink: Sink[T]): JavaSink[T] = {
    new JavaSink[T] {
      override def setup(context: Context): Unit = sink.setup(context)

      override def put(tuple: T): Unit = sink.put(tuple)

      override def cleanup(): Unit = sink.cleanup()
    }
  }

  def toJavaSource[T](source: Source[T]): JavaSource[T] = {
    new JavaSource[T] {
      override def setup(context: Context): Unit = source.setup(context)

      override def get(): Collection[T] =
        JavaConverters
          .asJavaCollectionConverter(source.get)
          .asJavaCollection

      override def cleanup(): Unit = source.cleanup()
    }
  }

  def toSerializableTransformer[R, T](
      transformer: SerializableTransformer[R, _ <: T])
    : JavaSerializableTransformer[R, _ <: T] = {

    def toScalaConsumerFunction[T](consumer: Consumer[T]): T => Unit =
      (t: T) => consumer.accept(t)

    new JavaSerializableTransformer[R, T] {
      override def setup(context: Context): Unit = transformer.setup(context)

      override def transform(r: R, consumer: Consumer[T]): Unit =
        transformer.transform(r, toScalaConsumerFunction[T](consumer))

      override def cleanup(): Unit = transformer.cleanup()
    }
  }

}
