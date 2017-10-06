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

package com.twitter.heron.dsl;

import com.twitter.heron.dsl.impl.BuilderImpl;

/**
 * Builder is used to register all sources. Builder thus keeps track
 * of all the starting points of the computation dag and uses this
 * information to build the topology
 */
public interface Builder {
  static Builder createBuilder() {
    return new BuilderImpl();
  }

  /**
   * All sources of the computation should register using addSource.
   * @param supplier The supplier function that is used to create the streamlet
   */
  <R> Streamlet<R> newSource(SerializableSupplier<R> supplier);

  /**
   * Same as above except returns KVStreamlet
   * @param supplier
   * @param <K>
   * @param <V>
   * @return
   */
  <K, V> KVStreamlet<K, V> newKVSource(SerializableSupplier<KeyValue<K, V>> supplier);

  /**
   * Creates a new Streamlet using the underlying generator
   * @param generator The generator that generates the tuples of the streamlet
   * @param <R>
   * @return
   */
  <R> Streamlet<R> newSource(SerializableGenerator<R> generator);

  /**
   * Same as above except returns a KVStreamlet
   * @param generator The genertor that generates the tuples of the streamlet
   * @param <K>
   * @param <V>
   * @return
   */
  <K, V> KVStreamlet<K, V> newKVSource(SerializableGenerator<KeyValue<K, V>> generator);
}
