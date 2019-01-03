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

package org.apache.heron.common.basics;

public abstract class ResourceMeasure<T extends ResourceMeasure, V extends Number & Comparable>
    implements Comparable<T> {

  protected final V value;

  protected ResourceMeasure(V value) {
    if (value == null) {
      throw new IllegalArgumentException();
    }
    this.value = value;
  }

  public V getValue() {
    return value;
  }

  public boolean isZero() {
    return value.doubleValue() == 0.0;
  }

  public abstract T minus(T other);

  public abstract T plus(T other);

  public abstract T multiply(int factor);

  public abstract T divide(int factor);

  public abstract T increaseBy(int percentage);

  @SuppressWarnings("unchecked")
  public boolean greaterThan(T other) {
    return value.compareTo(other.value) > 0;
  }

  @SuppressWarnings("unchecked")
  public boolean greaterOrEqual(T other) {
    return value.compareTo(other.value) >= 0;
  }

  @SuppressWarnings("unchecked")
  public boolean lessThan(T other) {
    return value.compareTo(other.value) < 0;
  }

  @SuppressWarnings("unchecked")
  public boolean lessOrEqual(T other) {
    return value.compareTo(other.value) <= 0;
  }

  @SuppressWarnings("unchecked")
  @Override
  public int compareTo(T o) {
    return value.compareTo(o.value);
  }
}
