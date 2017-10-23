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

import com.twitter.heron.streamlet.JoinType;
import com.twitter.heron.streamlet.KVStreamlet;
import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.KeyedWindow;
import com.twitter.heron.streamlet.SerializableBiFunction;
import com.twitter.heron.streamlet.SerializableBinaryOperator;
import com.twitter.heron.streamlet.SerializableSupplier;
import com.twitter.heron.streamlet.Source;
import com.twitter.heron.streamlet.WindowConfig;
import com.twitter.heron.streamlet.impl.streamlets.JoinStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.ReduceByKeyAndWindowStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.SourceKVStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.SupplierKVStreamlet;

/**
 * Some transformations like join and reduce assume a certain structure of the tuples
 * that it is processing. These transformations act on tuples of type KeyValue that have an
 * identifiable Key and Value components. Thus a KVStreamlet is just a special kind of Streamlet.
 */
public abstract class BaseKVStreamlet<K, V> extends BaseStreamlet<KeyValue<K, V>>
    implements KVStreamlet<K, V> {

  /**
   * Create a Streamlet based on the supplier function
   * @param supplier The Supplier function to generate the elements
   */
  static <K, V> BaseKVStreamlet<K, V> createSupplierKVStreamlet(
      SerializableSupplier<KeyValue<K, V>> supplier) {
    return new SupplierKVStreamlet<K, V>(supplier);
  }

  /**
   * Create a Streamlet based on the generator function
   * @param generator The Generator function to generate the elements
   */
  static <K, V> BaseKVStreamlet<K, V> createGeneratorKVStreamlet(
      Source<KeyValue<K, V>> generator) {
    return new SourceKVStreamlet<K, V>(generator);
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

    BaseKVStreamlet<K, V2> joinee = (BaseKVStreamlet<K, V2>) other;
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
