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

package com.twitter.heron.streamlet;

import java.util.List;

import com.twitter.heron.classification.InterfaceStability;

/**
 * Some transformations like join and reduce assume a certain structure of the tuples
 * that it is processing. These transformations act on tuples of type KeyValue that have an
 * identifiable Key and Value components. Thus a KVStreamlet is just a special kind of Streamlet.
 */
@InterfaceStability.Evolving
public interface KVStreamlet<K, V> extends BaseStreamlet<KVStreamlet<K, V>> {
  /**
   * Return a new KVStreamlet by applying mapFn to each element of this KVStreamlet
   * @param mapFn The Map Function that should be applied to each element
   */
  <K1, V1> KVStreamlet<K1, V1> map(SerializableFunction<? super KeyValue<? super K, ? super V>,
      ? extends KeyValue<? extends K1, ? extends V1>> mapFn);

  /**
   * Return a new Streamlet by applying mapFn to each element of this KVStreamlet
   * @param mapFn The Map Function that should be applied to each element
   */
  <R> Streamlet<R> mapToStreamlet(SerializableFunction<? super KeyValue<? super K, ? super V>,
                                  ? extends R> mapFn);

  /**
   * Return a new Streamlet by applying flatMapFn to each element of this Streamlet and
   * flattening the result
   * @param flatMapFn The FlatMap Function that should be applied to each element
   */
  <K1, V1> KVStreamlet<K1, V1> flatMap(
      SerializableFunction<? super KeyValue<? super K, ? super V>,
          ? extends Iterable<KeyValue<? extends K1, ? extends V1>>> flatMapFn);

  /**
   * Return a new Streamlet by applying the filterFn on each element of this streamlet
   * and including only those elements that satisfy the filterFn
   * @param filterFn The filter Function that should be applied to each element
   */
  KVStreamlet<K, V> filter(SerializablePredicate<? super KeyValue<? super K, ? super V>> filterFn);

  /**
   * Same as filter(filterFn).setNumPartitions(nPartitions) where filterFn is identity
   */
  KVStreamlet<K, V> repartition(int numPartitions);

  /**
   * A more generalized version of repartition where a user can determine which partitions
   * any particular tuple should go to
   */
  KVStreamlet<K, V> repartition(int numPartitions,
                                SerializableBiFunction<? super KeyValue<? super K, ? super V>,
                                    Integer, List<Integer>> partitionFn);

  /**
   * Clones the current Streamlet. It returns an array of numClones Streamlets where each
   * Streamlet contains all the tuples of the current Streamlet
   * @param numClones The number of clones to clone
   */
  List<KVStreamlet<K, V>> clone(int numClones);

  /**
   * Returns a new Streamlet thats the union of this and the ‘other’ streamlet. Essentially
   * the new streamlet will contain tuples belonging to both Streamlets
   */
  KVStreamlet<K, V> union(KVStreamlet<? extends K, ? extends V> other);

  /**
   * Returns a  new Streamlet by applying the transformFunction on each element of this streamlet.
   * Before starting to cycle the transformFunction over the Streamlet, the open function is called.
   * This allows the transform Function to do any kind of initialization/loading, etc.
   * @param serializableTransformer The transformation function to be applied
   * @return Streamlet containing the output of the transformFunction
   */
  <K1, V1> KVStreamlet<K1, V1> transform(
      SerializableTransformer<? super KeyValue<? super K, ? super V>,
          ? extends KeyValue<? extends K1, ? extends V1>> serializableTransformer);

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
  void consume(SerializableConsumer<? super KeyValue<? super K, ? super V>> consumer);

  /**
   * Applies the sink's put function to every element of the stream
   * This function does not return anything.
   * @param sink The Sink whose put method consumes each element
   * of this streamlet.
   */
  void toSink(Sink<? super KeyValue<? super K, ? super V>> sink);

  /**
   * Return a new KVStreamlet by inner joining 'this streamlet with ‘other’ streamlet.
   * The join is done over elements accumulated over a time window defined by TimeWindow.
   * @param other The Streamlet that we are joining with.
   * @param windowCfg This is a specification of what kind of windowing strategy you like to
   * have. Typical windowing strategies are sliding windows and tumbling windows
   * @param joinFunction The join function that needs to be applied
   */
  <V2, VR> KVStreamlet<KeyedWindow<K>, VR>
        join(KVStreamlet<K, V2> other, WindowConfig windowCfg,
             SerializableBiFunction<? super V, ? super V2, ? extends VR> joinFunction);


  /**
   * Return a new KVStreamlet by joining 'this streamlet with ‘other’ streamlet. The type of joining
   * is declared by the joinType parameter.
   * Types of joins {@link JoinType}
   * The join is done over elements accumulated over a time window defined by TimeWindow.
   * @param other The Streamlet that we are joining with.
   * @param windowCfg This is a specification of what kind of windowing strategy you like to
   * have. Typical windowing strategies are sliding windows and tumbling windows
   * @param joinType Type of Join. Options {@link JoinType}
   * @param joinFunction The join function that needs to be applied
   */
  <V2, VR> KVStreamlet<KeyedWindow<K>, VR>
        join(KVStreamlet<K, V2> other, WindowConfig windowCfg, JoinType joinType,
             SerializableBiFunction<? super V, ? super V2, ? extends VR> joinFunction);

  /**
   * Return a new Streamlet in which for each time_window, all elements are belonging to the
   * same key are reduced using the BinaryOperator and the result is emitted.
   * @param windowCfg This is a specification of what kind of windowing strategy you like to have.
   * Typical windowing strategies are sliding windows and tumbling windows
   * @param reduceFn The reduce function that you want to apply to all the values of a key.
   */
  KVStreamlet<KeyedWindow<K>, V> reduceByKeyAndWindow(WindowConfig windowCfg,
                                                      SerializableBinaryOperator<V> reduceFn);
}
