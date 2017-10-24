//  Copyright 2017 Twitter. All rights reserved.
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

package com.twitter.heron.streamlet.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.twitter.heron.streamlet.JoinType;
import com.twitter.heron.streamlet.KVStreamlet;
import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.KeyedWindow;
import com.twitter.heron.streamlet.SerializableBiFunction;
import com.twitter.heron.streamlet.SerializableBinaryOperator;
import com.twitter.heron.streamlet.SerializableConsumer;
import com.twitter.heron.streamlet.SerializableFunction;
import com.twitter.heron.streamlet.SerializablePredicate;
import com.twitter.heron.streamlet.SerializableSupplier;
import com.twitter.heron.streamlet.SerializableTransformer;
import com.twitter.heron.streamlet.Sink;
import com.twitter.heron.streamlet.Source;
import com.twitter.heron.streamlet.Streamlet;
import com.twitter.heron.streamlet.Window;
import com.twitter.heron.streamlet.WindowConfig;
import com.twitter.heron.streamlet.impl.streamlets.ConsumerStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.FilterStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.FlatMapStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.JoinStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.KVConsumerStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.KVFilterStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.KVFlatMapStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.KVLogStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.KVMapStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.KVRemapStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.KVSinkStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.KVTransformStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.KVUnionStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.LogStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.MapStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.ReduceByKeyAndWindowStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.ReduceByWindowStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.RemapStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.SinkStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.SourceKVStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.SupplierKVStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.TransformStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.UnionStreamlet;

import static java.util.function.DoubleUnaryOperator.identity;

/**
 * Some transformations like join and reduce assume a certain structure of the tuples
 * that it is processing. These transformations act on tuples of type KeyValue that have an
 * identifiable Key and Value components. Thus a KVStreamlet is just a special kind of Streamlet.
 */
public abstract class KVStreamletImpl<K, V> extends BaseStreamletImpl<KVStreamlet<K, V>>
    implements KVStreamlet<K, V> {

  @Override
  protected KVStreamletImpl<K, V> returnThis() {
    return this;
  }

  /**
   * Create a Streamlet based on the supplier function
   * @param supplier The Supplier function to generate the elements
   */
  static <K, V> KVStreamletImpl<K, V> createSupplierKVStreamlet(
      SerializableSupplier<KeyValue<K, V>> supplier) {
    return new SupplierKVStreamlet<K, V>(supplier);
  }

  /**
   * Create a Streamlet based on the generator function
   * @param generator The Generator function to generate the elements
   */
  static <K, V> KVStreamletImpl<K, V> createGeneratorKVStreamlet(
      Source<KeyValue<K, V>> generator) {
    return new SourceKVStreamlet<K, V>(generator);
  }

  /**
   * Return a new Streamlet by applying mapFn to each element of this Streamlet
   * @param mapFn The Map Function that should be applied to each element
   */
  @Override
  public <K1, V1> KVStreamlet<K1, V1> map(SerializableFunction<KeyValue<? super K, ? super V>,
      ? extends KeyValue<? extends K1, ? extends V1>> mapFn) {
    KVMapStreamlet<> retval = new KVMapStreamlet<K, V, K1, V1>(this, mapFn);
    addChild(retval);
    return retval;
  }

  /**
   * Return a new Streamlet by applying flatMapFn to each element of this Streamlet and
   * flattening the result
   * @param flatMapFn The FlatMap Function that should be applied to each element
   */
  @Override
  public <K1, V1> KVStreamlet<K1, V1> flatMap(
      SerializableFunction<KeyValue<? super K, ? super V>,
          ? extends Iterable<KeyValue<? extends K1, ? extends V1>>> flatMapFn) {
    KVFlatMapStreamlet<> retval = new KVFlatMapStreamlet<>(this, flatMapFn);
    addChild(retval);
    return retval;
  }

  /**
   * Return a new Streamlet by applying the filterFn on each element of this streamlet
   * and including only those elements that satisfy the filterFn
   * @param filterFn The filter Function that should be applied to each element
   */
  @Override
  public KVStreamlet<K, V> filter(SerializablePredicate<KeyValue<? super K, ? super V>> filterFn) {
    KVFilterStreamlet<R> retval = new KVFilterStreamlet<>(this, filterFn);
    addChild(retval);
    return retval;
  }

  /**
   * Same as filter(Identity).setNumPartitions(nPartitions)
   */
  @Override
  public KVStreamlet<K, V> repartition(int numPartitions) {
    return this.map((a) -> a).setNumPartitions(numPartitions);
  }

  /**
   * A more generalized version of repartition where a user can determine which partitions
   * any particular tuple should go to
   */
  @Override
  public KVStreamlet<K, V> repartition(int numPartitions,
                                  SerializableBiFunction<KeyValue<? super K, ? super V>,
                                      Integer, List<Integer>> partitionFn) {
    KVRemapStreamlet<> retval = new KVRemapStreamlet<>(this, partitionFn);
    retval.setNumPartitions(numPartitions);
    addChild(retval);
    return retval;
  }

  /**
   * Clones the current Streamlet. It returns an array of numClones Streamlets where each
   * Streamlet contains all the tuples of the current Streamlet
   * @param numClones The number of clones to clone
   */
  @Override
  public List<KVStreamlet<K, V>> clone(int numClones) {
    List<KVStreamlet<K, V>> retval = new ArrayList<>();
    for (int i = 0; i < numClones; ++i) {
      retval.add(repartition(getNumPartitions()));
    }
    return retval;
  }

  /**
   * Returns a new Streamlet thats the union of this and the ‘other’ streamlet. Essentially
   * the new streamlet will contain tuples belonging to both Streamlets
   */
  @Override
  public KVStreamlet<K, V> union(KVStreamlet<? extends K, ? extends V> other) {
    KVStreamletImpl<? extends K, ? extends V> joinee =
        (KVStreamletImpl<? extends K, ? extends V>) other;
    KVUnionStreamlet<K, V> retval = new KVUnionStreamlet<>(this, joinee);
    addChild(retval);
    joinee.addChild(retval);
    return retval;
  }

  /**
   * Logs every element of the streamlet using String.valueOf function
   * Note that LogStreamlet is an empty streamlet. That is its a streamlet
   * that does not contain any tuple. Thus this function returns void.
   */
  @Override
  public void log() {
    KVLogStreamlet<> logger = new KVLogStreamlet<>(this);
    addChild(logger);
    return;
  }

  /**
   * Applies the consumer function for every element of this streamlet
   * @param consumer The user supplied consumer function that is invoked for each element
   */
  @Override
  public void consume(SerializableConsumer<KeyValue<? super K, ? super V>> consumer) {
    KVConsumerStreamlet<K, V> consumerStreamlet = new KVConsumerStreamlet<>(this, consumer);
    addChild(consumerStreamlet);
    return;
  }

  /**
   * Uses the sink to consume every element of this streamlet
   * @param sink The Sink that consumes
   */
  @Override
  public void toSink(Sink<KeyValue<? super K, ? super V>> sink) {
    KVSinkStreamlet<> sinkStreamlet = new KVSinkStreamlet<>(this, sink);
    addChild(sinkStreamlet);
    return;
  }

  /**
   * Returns a  new Streamlet by applying the transformFunction on each element of this streamlet.
   * Before starting to cycle the transformFunction over the Streamlet, the open function is called.
   * This allows the transform Function to do any kind of initialization/loading, etc.
   * @param serializableTransformer The transformation function to be applied
   * @param <T> The return type of the transform
   * @return Streamlet containing the output of the transformFunction
   */
  @Override
  public <K1, V1> KVStreamlet<K1, V1> transform(
      SerializableTransformer<KeyValue<? super K, ? super V>,
          KeyValue<? extends K1, ? extends V1>> serializableTransformer) {
    KVTransformStreamlet<> transformStreamlet =
        new KVTransformStreamlet<>(this, serializableTransformer);
    addChild(transformStreamlet);
    return transformStreamlet;
  }

  /**
   * Return a new KVStreamlet by inner joining ‘this’ streamlet with ‘other’ streamlet.
   * The join is done over elements accumulated over a time window defined by TimeWindow.
   * @param other The Streamlet that we are joining with.
   * @param windowCfg This is a specification of what kind of windowing strategy you like to
   * have. Typical windowing strategies are sliding windows and tumbling windows
   * @param joinFunction The join function that needs to be applied
   */
  @Override
  public <V2, VR> KVStreamlet<KeyedWindow<K>, VR>
      join(KVStreamlet<K, V2> other, WindowConfig windowCfg,
           SerializableBiFunction<? super V, ? super V2, ? extends VR> joinFunction) {
    return join(other, windowCfg, JoinType.INNER, joinFunction);
  }

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
  @Override
  public <V2, VR> KVStreamlet<KeyedWindow<K>, VR>
        join(KVStreamlet<K, V2> other,
             WindowConfig windowCfg, JoinType joinType,
             SerializableBiFunction<? super V, ? super V2, ? extends VR> joinFunction) {

    KVStreamletImpl<K, V2> joinee = (KVStreamletImpl<K, V2>) other;
    JoinStreamlet<K, V, V2, VR> retval = JoinStreamlet.createJoinStreamlet(
        this, joinee, windowCfg, joinType, joinFunction);
    addChild(retval);
    joinee.addChild(retval);
    return retval;
  }

  /**
   * Return a new Streamlet in which for each time_window, all elements are belonging to the
   * same key are reduced using the BinaryOperator and the result is emitted.
   * @param windowCfg This is a specification of what kind of windowing strategy you like to have.
   * Typical windowing strategies are sliding windows and tumbling windows
   * @param reduceFn The reduce function that you want to apply to all the values of a key.
   */
  @Override
  public KVStreamlet<KeyedWindow<K>, V>
      reduceByKeyAndWindow(WindowConfig windowCfg, SerializableBinaryOperator<V> reduceFn) {
    ReduceByKeyAndWindowStreamlet<K, V> retval =
        new ReduceByKeyAndWindowStreamlet<>(this, windowCfg, reduceFn);
    addChild(retval);
    return retval;
  }
}
