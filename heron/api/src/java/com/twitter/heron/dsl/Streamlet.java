// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.dsl;

import java.util.List;

import com.twitter.heron.classification.InterfaceStability;

/**
 * A Streamlet is a (potentially unbounded) ordered collection of tuples.
 * Streamlets originate from pub/sub systems(such Pulsar/Kafka), or from
 * static data(such as csv files, HDFS files), or for that matter any other
 * source. They are also created by transforming existing Streamlets using
 * operations such as map/flatMap, etc.
 * Besides the tuples, a Streamlet has the following properties associated with it
 * a) name. User assigned or system generated name to refer the streamlet
 * b) nPartitions. Number of partitions that the streamlet is composed of. Thus the
 *    ordering of the tuples in a Streamlet is wrt the tuples within a partition.
 *    This allows the system to distribute  each partition to different nodes across the cluster.
 * A bunch of transformations can be done on Streamlets(like map/flatMap, etc.). Each
 * of these transformations operate on every tuple of the Streamlet and produce a new
 * Streamlet. One can think of a transformation attaching itself to the stream and processing
 * each tuple as they go by. Thus the parallelism of any operator is implicitly determined
 * by the number of partitions of the stream that it is operating on. If a particular
 * tranformation wants to operate at a different parallelism, one can repartition the
 * Streamlet before doing the transformation.
 */
@InterfaceStability.Evolving
public interface Streamlet<R> {
  /**
   * Sets the name of the Streamlet.
   * @param sName The name given by the user for this streamlet
   * @return Returns back the Streamlet with changed name
  */
  Streamlet<R> setName(String sName);

  /**
   * Gets the name of the Streamlet.
   * @return Returns the name of the Streamlet
   */
  String getName();

  /**
   * Sets the number of partitions of the streamlet
   * @param numPartitions The user assigned number of partitions
   * @return Returns back the Streamlet with changed number of partitions
   */
  Streamlet<R> setNumPartitions(int numPartitions);

  /**
   * Gets the number of partitions of this Streamlet.
   * @return the number of partitions of this Streamlet
   */
  int getNumPartitions();

  /**
   * Return a new Streamlet by applying mapFn to each element of this Streamlet
   * @param mapFn The Map Function that should be applied to each element
  */

  <T> Streamlet<T> map(SerializableFunction<? super R, ? extends T> mapFn);

  /**
   * Return a new KVStreamlet by applying mapFn to each element of this Streamlet.
   * This differs from the above map transformation in that it returns a KVStreamlet
   * instead of a plain Streamlet.
   * @param mapFn The Map function that should be applied to each element
  */
  <K, V> KVStreamlet<K, V> mapToKV(SerializableFunction<? super R, ? extends KeyValue<K, V>> mapFn);

  /**
   * Return a new Streamlet by applying flatMapFn to each element of this Streamlet and
   * flattening the result
   * @param flatMapFn The FlatMap Function that should be applied to each element
  */
  <T> Streamlet<T> flatMap(SerializableFunction<? super R, Iterable<? extends T>> flatMapFn);

  /**
   * Return a new KVStreamlet by applying map_function to each element of this Streamlet
   * and flattening the result. It differs from the above flatMap in that it returns a
   * KVStreamlet instead of a plain Streamlet
   * @param flatMapFn The FlatMap Function that should be applied to each element
  */
  <K, V> KVStreamlet<K, V> flatMapToKV(SerializableFunction<? super R,
      Iterable<? extends KeyValue<K, V>>> flatMapFn);

  /**
   * Return a new Streamlet by applying the filterFn on each element of this streamlet
   * and including only those elements that satisfy the filterFn
   * @param filterFn The filter Function that should be applied to each element
  */
  Streamlet<R> filter(SerializablePredicate<? super R> filterFn);

  /**
   * Same as filter(filterFn).setNumPartitions(nPartitions) where filterFn is identity
  */
  Streamlet<R> repartition(int numPartitions);

  /**
   * A more generalized version of repartition where a user can determine which partitions
   * any particular tuple should go to
   */
  Streamlet<R> repartition(int numPartitions,
                           SerializableBiFunction<? super R, Integer, List<Integer>> partitionFn);

  /**
   * Clones the current Streamlet. It returns an array of numClones Streamlets where each
   * Streamlet contains all the tuples of the current Streamlet
   * @param numClones The number of clones to clone
   */
  List<Streamlet<R>> clone(int numClones);

  /**
   * Returns a new Streamlet by accumulating tuples of this streamlet over a WindowConfig
   * windowConfig and applying reduceFn on those tuples
   * @param windowConfig This is a specification of what kind of windowing strategy you like
   * to have. Typical windowing strategies are sliding windows and tumbling windows
   * @param reduceFn The reduceFn to apply over the tuples accumulated on a window
   */
  KVStreamlet<Window, R> reduceByWindow(WindowConfig windowConfig,
                                        SerializableBinaryOperator<R> reduceFn);

  /**
   * Returns a new Streamlet thats the union of this and the ‘other’ streamlet. Essentially
   * the new streamlet will contain tuples belonging to both Streamlets
  */
  Streamlet<R> union(Streamlet<? extends R> other);

  /**
   * Returns a  new Streamlet by applying the transformFunction on each element of this streamlet.
   * Before starting to cycle the transformFunction over the Streamlet, the open function is called.
   * This allows the transform Function to do any kind of initialization/loading, etc.
   * @param serializableTransformer The transformation function to be applied
   * @param <T> The return type of the transform
   * @return Streamlet containing the output of the transformFunction
   */
  <T> Streamlet<T> transform(
      SerializableTransformer<? super R, ? extends T> serializableTransformer);

  /**
   * Logs every element of the streamlet using String.valueOf function
   * This is one of the sink functions in the sense that this operation returns void
   */
  void log();

  /**
   * Applies the consumer function to every element of the stream
   * This function does not return anything.
   * @param consumer The user supplied consumer function that is invoked for each element
   * of this streamlet.
   */
  void consume(SerializableConsumer<R> consumer);
}
