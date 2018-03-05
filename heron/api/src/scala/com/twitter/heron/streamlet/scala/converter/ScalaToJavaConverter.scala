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

import com.twitter.heron.streamlet.{
  Context,
  SerializableBiFunction,
  SerializableConsumer,
  SerializableFunction,
  SerializablePredicate,
  SerializableSupplier,
  Sink => JavaSink
}

import com.twitter.heron.streamlet.scala.Sink
import com.twitter.heron.streamlet.scala.Source
import java.lang.Iterable
import java.util.Collection
import scala.collection.JavaConverters._


/**
  * This class transforms passed User defined Scala Functions, Sources, Sinks
  * to related Java versions
  */
object ScalaToJavaConverter {

  def toSerializableSupplier[T](f: () => T) =
    new SerializableSupplier[T] {
      override def get(): T = f()
    }



  def toJavaSource[T](source: Source[T]): com.twitter.heron.streamlet.Source[T] = {
    new com.twitter.heron.streamlet.Source[T] {
      override def setup(context: Context): Unit = source.setup(context)

      override def get(): Collection[T] = scala.collection.JavaConverters.asJavaCollectionConverter(source.get).asJavaCollection

      override def cleanup(): Unit = source.cleanup()
    }
  }

  def toSerializableFunction[R, T](f: R => T) =
    new SerializableFunction[R, T] {

      override def apply(r: R): T = f(r)
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

  def toJavaSink[T](sink: Sink[T]): JavaSink[T] = {
    new JavaSink[T] {
      override def setup(context: Context): Unit = sink.setup(context)

      override def put(tuple: T): Unit = sink.put(tuple)

      override def cleanup(): Unit = sink.cleanup()
    }
  }

}
