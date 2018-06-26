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

import java.io.Serializable;

/**
 * Certain operations in the Streamlet API, like join/reduce, necessitate
 * the concept of key value pairs. This file defines a generic KeyValue
 * class. We make the KeyValue class serializable to allow it to be
 * serialized between components.
 */
public final class KeyValue<K, V> implements Serializable {
  private static final long serialVersionUID = -7120757965590727554L;
  private K key;
  private V value;

  public static <R, T> KeyValue<R, T> create(R k, T v) {
    return new KeyValue<R, T>(k, v);
  }

  KeyValue() {
    // nothing really
  }

  public KeyValue(K k, V v) {
    this.key = k;
    this.value = v;
  }
  public K getKey() {
    return key;
  }
  public void setKey(K k) {
    this.key = k;
  }
  public V getValue() {
    return value;
  }
  public void setValue(V v) {
    this.value = v;
  }

  @Override
  public String toString() {
    return "{ " + String.valueOf(key) + " : " + String.valueOf(value) + " }";
  }
}
