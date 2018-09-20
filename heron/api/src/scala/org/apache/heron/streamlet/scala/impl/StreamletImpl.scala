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
package org.apache.heron.streamlet.scala.impl

import scala.collection.JavaConverters

import org.apache.heron.api.bolt.IRichBolt
import org.apache.heron.streamlet.{
  JoinType,
  KeyValue,
  KeyedWindow,
  Streamlet => JavaStreamlet,
  WindowConfig
}
import org.apache.heron.streamlet.impl.{StreamletImpl => JavaStreamletImpl}
import org.apache.heron.streamlet.impl.streamlets.SupplierStreamlet

import org.apache.heron.streamlet.scala.{
  SerializableTransformer,
  Sink,
  Streamlet
}
import org.apache.heron.streamlet.scala.converter.ScalaToJavaConverter._

object StreamletImpl {

  def fromJavaStreamlet[R](javaStreamlet: JavaStreamlet[R]): Streamlet[R] =
    new StreamletImpl[R](javaStreamlet)

  def toJavaStreamlet[R](streamlet: Streamlet[R]): JavaStreamlet[R] =
    streamlet.asInstanceOf[StreamletImpl[R]].javaStreamlet

  /**
    * Create a Streamlet based on the supplier function
    *
    * @param supplier The Supplier function to generate the elements
    */
  private[impl] def createSupplierStreamlet[R](supplier: () => R) = {
    val serializableSupplier = toSerializableSupplier[R](supplier)
    val newJavaStreamlet =
      new SupplierStreamlet[R](serializableSupplier)
    fromJavaStreamlet[R](newJavaStreamlet)
  }

}

/**
  * This class provides Scala Streamlet Implementation by wrapping Java Streamlet API.
  * Passed User defined Scala Functions are transformed to related FunctionalInterface versions and
  * related Java Streamlet is transformed to Scala version again.
  */
class StreamletImpl[R](val javaStreamlet: JavaStreamlet[R])
    extends Streamlet[R] {

  import StreamletImpl._

  /**
    * Sets the name of the Streamlet.
    *
    * @param sName The name given by the user for this Streamlet
    * @return Returns back the Streamlet with changed name
    */
  override def setName(sName: String): Streamlet[R] =
    fromJavaStreamlet[R](javaStreamlet.setName(sName))

  /**
    * Gets the name of the Streamlet.
    *
    * @return Returns the name of the Streamlet
    */
  override def getName(): String = javaStreamlet.getName

  /**
    * Sets the number of partitions of the streamlet
    *
    * @param numPartitions The user assigned number of partitions
    * @return Returns back the Streamlet with changed number of partitions
    */
  override def setNumPartitions(numPartitions: Int): Streamlet[R] =
    fromJavaStreamlet[R](javaStreamlet.setNumPartitions(numPartitions))

  /**
    * Gets the number of partitions of this Streamlet.
    *
    * @return the number of partitions of this Streamlet
    */
  override def getNumPartitions(): Int = javaStreamlet.getNumPartitions

  /**
    * Return a new Streamlet by applying mapFn to each element of this Streamlet
    *
    * @param mapFn The Map Function that should be applied to each element
    */
  override def map[T](mapFn: R => T): Streamlet[T] = {
    val serializableFunction = toSerializableFunction[R, T](mapFn)
    val newJavaStreamlet = javaStreamlet.map[T](serializableFunction)
    fromJavaStreamlet[T](newJavaStreamlet)
  }

  /**
    * Return a new Streamlet by applying flatMapFn to each element of this Streamlet and
    * flattening the result
    *
    * @param flatMapFn The FlatMap Function that should be applied to each element
    */
  override def flatMap[T](flatMapFn: R => Iterable[_ <: T]): Streamlet[T] = {
    val serializableFunction =
      toSerializableFunctionWithIterable[R, T](flatMapFn)
    val newJavaStreamlet = javaStreamlet.flatMap[T](serializableFunction)
    fromJavaStreamlet[T](newJavaStreamlet)
  }

  /**
    * Return a new Streamlet by applying a user provided bolt to each element of this Streamlet
    *
    * @param bolt The rich bolt object that should be applied to each element
    */
  override def applyBolt[T](bolt: IRichBolt): Streamlet[T] = {
    val newJavaStreamlet = javaStreamlet.applyBolt[T](bolt)
    fromJavaStreamlet[T](newJavaStreamlet)
  }

  /**
    * Return a new Streamlet by applying the filterFn on each element of this streamlet
    * and including only those elements that satisfy the filterFn
    *
    * @param filterFn The filter Function that should be applied to each element
    */
  override def filter(filterFn: R => Boolean): Streamlet[R] = {
    val serializablePredicate = toSerializablePredicate[R](filterFn)
    val newJavaStreamlet = javaStreamlet.filter(serializablePredicate)
    fromJavaStreamlet[R](newJavaStreamlet)
  }

  /**
    * Same as filter(filterFn).setNumPartitions(nPartitions) where filterFn is identity
    */
  override def repartition(numPartitions: Int): Streamlet[R] = {
    val newJavaStreamlet = javaStreamlet.repartition(numPartitions)
    fromJavaStreamlet[R](newJavaStreamlet)
  }

  /**
    * A more generalized version of repartition where a user can determine which partitions
    * any particular tuple should go to. For each element of the current streamlet, the user
    * supplied partitionFn is invoked passing in the element as the first argument. The second
    * argument is the number of partitions of the downstream streamlet. The partitionFn should
    * return 0 or more unique numbers between 0 and n partitions to indicate which partitions
    * this element should be routed to.
    */
  override def repartition(numPartitions: Int,
                           partitionFn: (R, Int) => Seq[Int]): Streamlet[R] = {
    val partitionFunction = toSerializableBiFunctionWithSeq[R](partitionFn)
    val newJavaStreamlet =
      javaStreamlet.repartition(numPartitions, partitionFunction)
    fromJavaStreamlet[R](newJavaStreamlet)
  }

  /**
    * Clones the current Streamlet. It returns an array of numClones Streamlets where each
    * Streamlet contains all the tuples of the current Streamlet
    *
    * @param numClones The number of clones to clone
    */
  override def clone(numClones: Int): Seq[Streamlet[R]] = {
    val javaClonedStreamlets = javaStreamlet.clone(numClones)
    val javaClonedStreamletsAsScalaList = JavaConverters
      .asScalaBufferConverter(javaClonedStreamlets)
      .asScala
    javaClonedStreamletsAsScalaList.map(streamlet =>
      fromJavaStreamlet[R](streamlet))
  }

  /**
    * Return a new Streamlet by inner joining 'this streamlet with ‘other’ streamlet.
    * The join is done over elements accumulated over a time window defined by windowCfg.
    * The elements are compared using the thisKeyExtractor for this streamlet with the
    * otherKeyExtractor for the other streamlet. On each matching pair, the joinFunction is applied.
    *
    * @param other             The Streamlet that we are joining with.
    * @param thisKeyExtractor  The function applied to a tuple of this streamlet to get the key
    * @param otherKeyExtractor The function applied to a tuple of the other streamlet to get the key
    * @param windowCfg         This is a specification of what kind of windowing strategy you like to
    * have. Typical windowing strategies are sliding windows and tumbling windows
    * @param joinFunction      The join function that needs to be applied
    */
  override def join[K, S, T](
      other: Streamlet[S],
      thisKeyExtractor: R => K,
      otherKeyExtractor: S => K,
      windowCfg: WindowConfig,
      joinFunction: (R, S) => T): Streamlet[KeyValue[KeyedWindow[K], T]] = {
    val javaOtherStreamlet = toJavaStreamlet[S](other)
    val javaThisKeyExtractor = toSerializableFunction[R, K](thisKeyExtractor)
    val javaOtherKeyExtractor = toSerializableFunction[S, K](otherKeyExtractor)
    val javaJoinFunction = toSerializableBiFunction[R, S, T](joinFunction)

    val newJavaStreamlet = javaStreamlet.join[K, S, T](javaOtherStreamlet,
                                                       javaThisKeyExtractor,
                                                       javaOtherKeyExtractor,
                                                       windowCfg,
                                                       javaJoinFunction)
    fromJavaStreamlet[KeyValue[KeyedWindow[K], T]](newJavaStreamlet)
  }

  /**
    * Return a new KVStreamlet by joining 'this streamlet with ‘other’ streamlet. The type of joining
    * is declared by the joinType parameter.
    * The join is done over elements accumulated over a time window defined by windowCfg.
    * The elements are compared using the thisKeyExtractor for this streamlet with the
    * otherKeyExtractor for the other streamlet. On each matching pair, the joinFunction is applied.
    * Types of joins {@link JoinType}
    *
    * @param other             The Streamlet that we are joining with.
    * @param thisKeyExtractor  The function applied to a tuple of this streamlet to get the key
    * @param otherKeyExtractor The function applied to a tuple of the other streamlet to get the key
    * @param windowCfg         This is a specification of what kind of windowing strategy you like to
    * have. Typical windowing strategies are sliding windows and tumbling windows
    * @param joinType          Type of Join. Options { @link JoinType}
    * @param joinFunction      The join function that needs to be applied
    */
  override def join[K, S, T](
      other: Streamlet[S],
      thisKeyExtractor: R => K,
      otherKeyExtractor: S => K,
      windowCfg: WindowConfig,
      joinType: JoinType,
      joinFunction: (R, S) => T): Streamlet[KeyValue[KeyedWindow[K], T]] = {
    val javaOtherStreamlet = toJavaStreamlet[S](other)
    val javaThisKeyExtractor = toSerializableFunction[R, K](thisKeyExtractor)
    val javaOtherKeyExtractor = toSerializableFunction[S, K](otherKeyExtractor)
    val javaJoinFunction = toSerializableBiFunction[R, S, T](joinFunction)

    val newJavaStreamlet = javaStreamlet.join[K, S, T](javaOtherStreamlet,
                                                       javaThisKeyExtractor,
                                                       javaOtherKeyExtractor,
                                                       windowCfg,
                                                       joinType,
                                                       javaJoinFunction)
    fromJavaStreamlet[KeyValue[KeyedWindow[K], T]](newJavaStreamlet)
  }

  /**
    * Return a new Streamlet accumulating tuples of this streamlet over a Window defined by
    * windowCfg and applying reduceFn on those tuples.
    *
    * @param keyExtractor   The function applied to a tuple of this streamlet to get the key
    * @param valueExtractor The function applied to a tuple of this streamlet to extract the value
    *                       to be reduced on
    * @param windowCfg      This is a specification of what kind of windowing strategy you like to have.
    *                       Typical windowing strategies are sliding windows and tumbling windows
    * @param reduceFn       The reduce function that you want to apply to all the values of a key.
    */
  override def reduceByKeyAndWindow[K, V](
      keyExtractor: R => K,
      valueExtractor: R => V,
      windowCfg: WindowConfig,
      reduceFn: (V, V) => V): Streamlet[KeyValue[KeyedWindow[K], V]] = {
    val javaKeyExtractor = toSerializableFunction[R, K](keyExtractor)
    val javaValueExtractor = toSerializableFunction[R, V](valueExtractor)
    val javaReduceFunction = toSerializableBinaryOperator[V](reduceFn)

    val newJavaStreamlet = javaStreamlet.reduceByKeyAndWindow[K, V](
      javaKeyExtractor,
      javaValueExtractor,
      windowCfg,
      javaReduceFunction)
    fromJavaStreamlet[KeyValue[KeyedWindow[K], V]](newJavaStreamlet)
  }

  /**
    * Return a new Streamlet accumulating tuples of this streamlet over a Window defined by
    * windowCfg and applying reduceFn on those tuples. For each window, the value identity is used
    * as a initial value. All the matching tuples are reduced using reduceFn startin from this
    * initial value.
    *
    * @param keyExtractor The function applied to a tuple of this streamlet to get the key
    * @param windowCfg    This is a specification of what kind of windowing strategy you like to have.
    *                     Typical windowing strategies are sliding windows and tumbling windows
    * @param identity     The identity element is both the initial value inside the reduction window
    *                     and the default result if there are no elements in the window
    * @param reduceFn     The reduce function takes two parameters: a partial result of the reduction
    *                     and the next element of the stream. It returns a new partial result.
    */
  override def reduceByKeyAndWindow[K, T](
      keyExtractor: R => K,
      windowCfg: WindowConfig,
      identity: T,
      reduceFn: (T, R) => T): Streamlet[KeyValue[KeyedWindow[K], T]] = {
    val javaKeyExtractor = toSerializableFunction[R, K](keyExtractor)
    val javaReduceFunction = toSerializableBiFunction[T, R, T](reduceFn)

    val newJavaStreamlet = javaStreamlet.reduceByKeyAndWindow[K, T](
      javaKeyExtractor,
      windowCfg,
      identity,
      javaReduceFunction)
    fromJavaStreamlet[KeyValue[KeyedWindow[K], T]](newJavaStreamlet)
  }

  /**
    * Returns a new Streamlet that is the union of this and the ‘other’ streamlet. Essentially
    * the new streamlet will contain tuples belonging to both Streamlets
    */
  override def union(other: Streamlet[_ <: R]): Streamlet[R] = {
    val newJavaStreamlet = javaStreamlet.union(toJavaStreamlet(other))
    fromJavaStreamlet(newJavaStreamlet)
  }

  /**
    * Returns a  new Streamlet by applying the transformFunction on each element of this streamlet.
    * Before starting to cycle the transformFunction over the Streamlet, the open function is called.
    * This allows the transform Function to do any kind of initialization/loading, etc.
    *
    * @param serializableTransformer The transformation function to be applied
    * @param <                       T> The return type of the transform
    * @return Streamlet containing the output of the transformFunction
    */
  override def transform[T](
      serializableTransformer: SerializableTransformer[R, _ <: T])
    : Streamlet[T] = {
    val javaSerializableTransformer =
      toSerializableTransformer[R, T](serializableTransformer)
    val newJavaStreamlet =
      javaStreamlet.transform[T](javaSerializableTransformer)
    fromJavaStreamlet(newJavaStreamlet)
  }

  /**
    * Logs every element of the streamlet using String.valueOf function
    * This is one of the sink functions in the sense that this operation returns void
    */
  override def log(): Unit = javaStreamlet.log()

  /**
    * Applies the consumer function to every element of the stream
    * This function does not return anything.
    *
    * @param consumer The user supplied consumer function that is invoked for each element
    *                 of this streamlet.
    */
  override def consume(consumer: R => Unit): Unit = {
    val serializableConsumer = toSerializableConsumer[R](consumer)
    javaStreamlet.consume(serializableConsumer)
  }

  /**
    * Applies the sink's put function to every element of the stream
    * This function does not return anything.
    *
    * @param sink The Sink whose put method consumes each element
    *             of this streamlet.
    */
  override def toSink(sink: Sink[R]): Unit = {
    val javaSink = toJavaSink[R](sink)
    javaStreamlet.toSink(javaSink)
  }

  /**
    * Gets all the children of this streamlet.
    * Children of a streamlet are streamlets that are resulting from transformations of elements of
    * this and potentially other streamlets.
    *
    * @return The kid streamlets
    */
  private[impl] def getChildren: List[JavaStreamletImpl[_]] = {
    import _root_.scala.collection.JavaConversions._
    val children =
      javaStreamlet
        .asInstanceOf[JavaStreamletImpl[_]]
        .getChildren
    children.toList
  }
}
