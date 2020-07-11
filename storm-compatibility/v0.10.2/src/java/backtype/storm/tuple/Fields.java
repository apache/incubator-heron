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

package backtype.storm.tuple;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

public class Fields implements Iterable<String>, Serializable {
  private static final long serialVersionUID = -7503973062110993975L;
  private org.apache.heron.api.tuple.Fields delegate;

  public Fields(String... fields) {
    delegate = new org.apache.heron.api.tuple.Fields(fields);
  }

  public Fields(org.apache.heron.api.tuple.Fields delegate) {
    this.delegate = delegate;
  }

  public Fields(List<String> fields) {
    delegate = new org.apache.heron.api.tuple.Fields(fields);
  }

  public List<Object> select(Fields selector, List<Object> tuple) {
    return delegate.select(selector.getDelegate(), tuple);
  }

  public List<String> toList() {
    return delegate.toList();
  }

  public int size() {
    return delegate.size();
  }

  public String get(int index) {
    return delegate.get(index);
  }

  public Iterator<String> iterator() {
    return delegate.iterator();
  }

  /**
   * Returns the position of the specified field.
   */
  public int fieldIndex(String field) {
    return delegate.fieldIndex(field);
  }

  /**
   * Returns true if this contains the specified name of the field.
   */
  public boolean contains(String field) {
    return delegate.contains(field);
  }

  public String toString() {
    return delegate.toString();
  }

  public org.apache.heron.api.tuple.Fields getDelegate() {
    return delegate;
  }
}
