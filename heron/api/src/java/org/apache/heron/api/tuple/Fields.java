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

package org.apache.heron.api.tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Fields implements Iterable<String>, Serializable {
  private static final long serialVersionUID = -1045737418722082345L;

  private List<String> fields;
  private Map<String, Integer> mIndex = new ConcurrentHashMap<String, Integer>();

  public Fields(String... pFields) {
    this(Arrays.asList(pFields));
  }

  public Fields(List<String> pFields) {
    fields = new ArrayList<String>(pFields.size());
    for (String field : pFields) {
      if (fields.contains(field)) {
        throw new IllegalArgumentException(
            String.format("duplicate field '%s'", field)
        );
      }
      fields.add(field);
    }
    index();
  }

  public List<Object> select(Fields selector, List<Object> tuple) {
    List<Object> ret = new ArrayList<Object>(selector.size());
    for (String s : selector) {
      ret.add(tuple.get(mIndex.get(s)));
    }
    return ret;
  }

  public List<String> toList() {
    return new ArrayList<String>(fields);
  }

  public int size() {
    return fields.size();
  }

  public String get(int index) {
    return fields.get(index);
  }

  public Iterator<String> iterator() {
    return fields.iterator();
  }

  /**
   * Returns the position of the specified field.
   */
  public int fieldIndex(String field) {
    Integer ret = mIndex.get(field);
    if (ret == null) {
      throw new IllegalArgumentException(field + " does not exist");
    }
    return ret;
  }

  /**
   * Returns true if this contains the specified name of the field.
   */
  public boolean contains(String field) {
    return mIndex.containsKey(field);
  }

  private void index() {
    for (int i = 0; i < fields.size(); i++) {
      mIndex.put(fields.get(i), i);
    }
  }

  @Override
  public String toString() {
    return fields.toString();
  }
}
