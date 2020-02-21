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

package org.apache.heron.streamlet;

import java.util.List;
import java.util.Map;

import org.apache.heron.api.grouping.StreamGrouping;
import org.apache.heron.classification.InterfaceStability;

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
 * transformation wants to operate at a different parallelism, one can repartition the
 * Streamlet before doing the transformation.
 */
@InterfaceStability.Evolving
public interface Streamlet<R> extends StreamletBase<R> {

  /**
   * Sets the name of the BaseStreamlet.
   * @param sName The name given by the user for this BaseStreamlet
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
   * Set the id of the stream to be used by the children nodes.
   * Usage (assuming source is a Streamlet object with two output streams: stream1 and stream2):
   *   source.withStream("stream1").filter(...).log();
   *   source.withStream("stream2").filter(...).log();
   * @param streamId The specified stream id
   * @return Returns back the Streamlet with changed stream id
   */
  Streamlet<R> withStream(String streamId);

  /**
   * Gets the stream id of this Streamlet.
   * @return the stream id of this Streamlet
   */
  String getStreamId();

  /**
   * Return a new Streamlet by applying mapFn to each element of this Streamlet
   * @param mapFn The Map Function that should be applied to each element
  */
  <T> Streamlet<T> map(SerializableFunction<R, ? extends T> mapFn);

  /**
   * Return a new Streamlet by applying flatMapFn to each element of this Streamlet and
   * flattening the result
   * @param flatMapFn The FlatMap Function that should be applied to each element
   */
  <T> Streamlet<T> flatMap(
      SerializableFunction<R, ? extends Iterable<? extends T>> flatMapFn);

  /**
   * Return a new Streamlet by applying the filterFn on each element of this streamlet
   * and including only those elements that satisfy the filterFn
   * @param filterFn The filter Function that should be applied to each element
  */
  Streamlet<R> filter(SerializablePredicate<R> filterFn);

  /**
   * Same as filter(filterFn).setNumPartitions(nPartitions) where filterFn is identity
  */
  Streamlet<R> repartition(int numPartitions);

  /**
   * A more generalized version of repartition where a user can determine which partitions
   * any particular tuple should go to. For each element of the current streamlet, the user
   * supplied partitionFn is invoked passing in the element as the first argument. The second
   * argument is the number of partitions of the downstream streamlet. The partitionFn should
   * return 0 or more unique numbers between 0 and npartitions to indicate which partitions
   * this element should be routed to.
   */
  Streamlet<R> repartition(int numPartitions,
                           SerializableBiFunction<R, Integer, List<Integer>> partitionFn);

  /**
   * Clones the current Streamlet. It returns an array of numClones Streamlets where each
   * Streamlet contains all the tuples of the current Streamlet
   * @param numClones The number of clones to clone
   */
  List<Streamlet<R>> clone(int numClones);

  /**
   * Return a new Streamlet by inner joining 'this streamlet with ‘other’ streamlet.
   * The join is done over elements accumulated over a time window defined by windowCfg.
   * The elements are compared using the thisKeyExtractor for this streamlet with the
   * otherKeyExtractor for the other streamlet. On each matching pair, the joinFunction is applied.
   * @param other The Streamlet that we are joining with.
   * @param thisKeyExtractor The function applied to a tuple of this streamlet to get the key
   * @param otherKeyExtractor The function applied to a tuple of the other streamlet to get the key
   * @param windowCfg This is a specification of what kind of windowing strategy you like to
   * have. Typical windowing strategies are sliding windows and tumbling windows
   * @param joinFunction The join function that needs to be applied
   */
  <K, S, T> KVStreamlet<KeyedWindow<K>, T>
        join(Streamlet<S> other, SerializableFunction<R, K> thisKeyExtractor,
             SerializableFunction<S, K> otherKeyExtractor, WindowConfig windowCfg,
             SerializableBiFunction<R, S, ? extends T> joinFunction);


  /**
   * Return a new KVStreamlet by joining 'this streamlet with ‘other’ streamlet. The type of joining
   * is declared by the joinType parameter.
   * The join is done over elements accumulated over a time window defined by windowCfg.
   * The elements are compared using the thisKeyExtractor for this streamlet with the
   * otherKeyExtractor for the other streamlet. On each matching pair, the joinFunction is applied.
   * Types of joins {@link JoinType}
   * @param other The Streamlet that we are joining with.
   * @param thisKeyExtractor The function applied to a tuple of this streamlet to get the key
   * @param otherKeyExtractor The function applied to a tuple of the other streamlet to get the key
   * @param windowCfg This is a specification of what kind of windowing strategy you like to
   * have. Typical windowing strategies are sliding windows and tumbling windows
   * @param joinType Type of Join. Options {@link JoinType}
   * @param joinFunction The join function that needs to be applied
   */
  <K, S, T> KVStreamlet<KeyedWindow<K>, T>
        join(Streamlet<S> other, SerializableFunction<R, K> thisKeyExtractor,
             SerializableFunction<S, K> otherKeyExtractor, WindowConfig windowCfg,
             JoinType joinType, SerializableBiFunction<R, S, ? extends T> joinFunction);

  /**
   * Return a new Streamlet accumulating tuples of this streamlet and applying reduceFn on those tuples.
   * @param keyExtractor The function applied to a tuple of this streamlet to get the key
   * @param valueExtractor The function applied to a tuple of this streamlet to extract the value
   * to be reduced on
   * @param reduceFn The reduce function that you want to apply to all the values of a key.
   */
  <K, T> KVStreamlet<K, T> reduceByKey(SerializableFunction<R, K> keyExtractor,
                                       SerializableFunction<R, T> valueExtractor,
                                       SerializableBinaryOperator<T> reduceFn);

  /**
   * Return a new Streamlet accumulating tuples of this streamlet and applying reduceFn on those tuples.
   * @param keyExtractor The function applied to a tuple of this streamlet to get the key
   * @param identity The identity element is the initial value for each key
   * @param reduceFn The reduce function that you want to apply to all the values of a key.
   */
  <K, T> KVStreamlet<K, T> reduceByKey(SerializableFunction<R, K> keyExtractor,
                                       T identity,
                                       SerializableBiFunction<T, R, ? extends T> reduceFn);

  /**
   * Return a new Streamlet accumulating tuples of this streamlet over a Window defined by
   * windowCfg and applying reduceFn on those tuples.
   * @param keyExtractor The function applied to a tuple of this streamlet to get the key
   * @param valueExtractor The function applied to a tuple of this streamlet to extract the value
   * to be reduced on
   * @param windowCfg This is a specification of what kind of windowing strategy you like to have.
   * Typical windowing strategies are sliding windows and tumbling windows
   * @param reduceFn The reduce function that you want to apply to all the values of a key.
   */
  <K, V> KVStreamlet<KeyedWindow<K>, V> reduceByKeyAndWindow(
      SerializableFunction<R, K> keyExtractor, SerializableFunction<R, V> valueExtractor,
      WindowConfig windowCfg, SerializableBinaryOperator<V> reduceFn);

  /**
   * Return a new Streamlet accumulating tuples of this streamlet over a Window defined by
   * windowCfg and applying reduceFn on those tuples. For each window, the value identity is used
   * as a initial value. All the matching tuples are reduced using reduceFn startin from this
   * initial value.
   * @param keyExtractor The function applied to a tuple of this streamlet to get the key
   * @param windowCfg This is a specification of what kind of windowing strategy you like to have.
   * Typical windowing strategies are sliding windows and tumbling windows
   * @param identity The identity element is both the initial value inside the reduction window
   * and the default result if there are no elements in the window
   * @param reduceFn The reduce function takes two parameters: a partial result of the reduction
   * and the next element of the stream. It returns a new partial result.
   */
  <K, T> KVStreamlet<KeyedWindow<K>, T> reduceByKeyAndWindow(
      SerializableFunction<R, K> keyExtractor, WindowConfig windowCfg,
      T identity, SerializableBiFunction<T, R, ? extends T> reduceFn);

  /**
   * Returns a new Streamlet that is the union of this and the ‘other’ streamlet. Essentially
   * the new streamlet will contain tuples belonging to both Streamlets
  */
  Streamlet<R> union(Streamlet<? extends R> other);

  /**
   * Returns a new Streamlet by applying the transformFunction on each element of this streamlet.
   * Before starting to cycle the transformFunction over the Streamlet, the open function is called.
   * This allows the transform Function to do any kind of initialization/loading, etc.
   * @param serializableTransformer The transformation function to be applied
   * @param <T> The return type of the transform
   * @return Streamlet containing the output of the transformFunction
   */
  <T> Streamlet<T> transform(
      SerializableTransformer<R, ? extends T> serializableTransformer);

  /**
   * Returns a new Streamlet by applying the operator on each element of this streamlet.
   * @param operator The operator to be applied
   * @param <T> The return type of the transform
   * @return Streamlet containing the output of the operation
   */
  <T> Streamlet<T> applyOperator(IStreamletOperator<R, T> operator);

  /**
   * Returns a new Streamlet by applying the operator on each element of this streamlet.
   * @param operator The operator to be applied
   * @param grouper The grouper to be applied with the operator
   * @param <T> The return type of the transform
   * @return Streamlet containing the output of the operation
   */
  <T> Streamlet<T> applyOperator(IStreamletOperator<R, T> operator, StreamGrouping grouper);

  /**
   * Returns multiple streams by splitting incoming stream.
   * @param splitFns The Split Functions that test if the tuple should be emitted into each stream
   * Note that there could be 0 or multiple target stream ids
   */
  Streamlet<R> split(Map<String, SerializablePredicate<R>> splitFns);

  /**
   * Return a new KVStreamlet<K, R> by applying key extractor to each element of this Streamlet
   * @param keyExtractor The function applied to a tuple of this streamlet to get the key
   */
  <K> KVStreamlet<K, R> keyBy(SerializableFunction<R, K> keyExtractor);

  /**
   * Return a new KVStreamlet<K, V> by applying key and value extractor to each element of this
   * Streamlet
   * @param keyExtractor The function applied to a tuple of this streamlet to get the key
   * @param valueExtractor The function applied to a tuple of this streamlet to extract the value
   */
  <K, V> KVStreamlet<K, V> keyBy(SerializableFunction<R, K> keyExtractor,
                                 SerializableFunction<R, V> valueExtractor);

  /**
   * Returns a new stream of <key, count> by counting tuples in this stream on each key.
   * @param keyExtractor The function applied to a tuple of this streamlet to get the key
   */
  <K> KVStreamlet<K, Long> countByKey(SerializableFunction<R, K> keyExtractor);

  /**
   * Returns a new stream of <key, count> by counting tuples over a window in this stream on each key.
   * @param keyExtractor The function applied to a tuple of this streamlet to get the key
   * @param windowCfg This is a specification of what kind of windowing strategy you like to have.
   * Typical windowing strategies are sliding windows and tumbling windows
   * Note that there could be 0 or multiple target stream ids
   */
  <K> KVStreamlet<KeyedWindow<K>, Long> countByKeyAndWindow(
      SerializableFunction<R, K> keyExtractor, WindowConfig windowCfg);

  /**
   * Logs every element of the streamlet using String.valueOf function
   * This is one of the sink functions in the sense that this operation returns void
   */
  StreamletBase<R> log();

  /**
   * Applies the consumer function to every element of the stream
   * This function does not return anything.
   * @param consumer The user supplied consumer function that is invoked for each element
   * of this streamlet.
   */
  StreamletBase<R> consume(SerializableConsumer<R> consumer);

  /**
   * Applies the sink's put function to every element of the stream
   * This function does not return anything.
   * @param sink The Sink whose put method consumes each element
   * of this streamlet.
   */
  StreamletBase<R> toSink(Sink<R> sink);
}
