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

package com.twitter.heron.dsl.impl.streamlets;

import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

import com.twitter.heron.dsl.KVStreamlet;
import com.twitter.heron.dsl.KeyValue;
import com.twitter.heron.dsl.WindowConfig;

/**
 * Some transformations like join and reduce assume a certain structure of the tuples
 * that it is processing. These transformations act on tuples of type KeyValue that have an
 * identifiable Key and Value components. Thus a KVStreamlet is just a special kind of Streamlet.
 */
public abstract class KVStreamletImpl<K, V> extends StreamletImpl<KeyValue<K, V>>
    implements KVStreamlet<K, V> {
  /**
   * Return a new KVStreamlet by joining ‘other’ streamlet with ‘this’ streamlet.
   * The join is done over elements accumulated over a time window defined by TimeWindow.
   * @param other The Streamlet that we are joining with.
   * @param windowCfg This is a specification of what kind of windowing strategy you like to
   * have. Typical windowing strategies are sliding windows and tumbling windows
   * @param joinFunction The join function that needs to be applied
  */
  public <V2, VR> KVStreamlet<K, VR> join(KVStreamlet<K, V2> other,
                                   WindowConfig windowCfg,
                                   BiFunction<V, V2, VR> joinFunction) {
    KVStreamletImpl<K, V2> joinee = (KVStreamletImpl<K, V2>) other;
    return new JoinStreamlet<K, V, V2, VR>(this, joinee, windowCfg, joinFunction);
  }

  /**
   * Return a new Streamlet in which for each time_window, all elements are belonging to the
   * same key are reduced using the BinaryOperator and the result is emitted.
   * @param windowCfg This is a specification of what kind of windowing strategy you like to have.
   * Typical windowing strategies are sliding windows and tumbling windows
   * @param reduceFn The reduce function that you want to apply to all the values of a key.
   */
  public KVStreamlet<K, V> reduceByKeyAndWindow(WindowConfig windowCfg,
                                                BinaryOperator<V> reduceFn) {
    return new ReduceByKeyAndWindowStreamlet<K, V>(this, windowCfg, reduceFn);
  }
}
