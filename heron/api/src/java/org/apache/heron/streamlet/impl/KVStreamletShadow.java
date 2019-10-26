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

package org.apache.heron.streamlet.impl.streamlets;

import org.apache.heron.streamlet.KVStreamlet;
import org.apache.heron.streamlet.KeyValue;
import org.apache.heron.streamlet.impl.StreamletImpl;

/**
 * KVStreamletShadow is a decorator for StreamletImpl<KeyValue<?, ?>> objects.
 * Please check StreamShadow comments for more details.
 *
 * Usage:
 * To create a shadow object that selecting "test" stream from an existing
 * StreamletImpl<KeyValue<K, V>> object(stream):
 *
 * KVStreamlet<K, V> kv = new KVStreamletShadow<K, V>(stream)
 *
 */
public class KVStreamletShadow<K, V>
    extends StreamletShadow<KeyValue<K, V>>
    implements KVStreamlet<K, V> {

  // Note that KVStreamletShadow is constructed from StreamletImpl
  public KVStreamletShadow(StreamletImpl<KeyValue<K, V>> real) {
    super(real);
  }

  /*
   * Functions accessible by child objects need to be overriden (forwarding the call to
   * the real object since shadow object shouldn't have them)
   */
  @Override
  public KVStreamletShadow<K, V> setName(String sName) {
    super.setName(sName);
    return this;
  }

  @Override
  public KVStreamletShadow<K, V> setNumPartitions(int numPartitions) {
    super.setNumPartitions(numPartitions);
    return this;
  }

}
