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

package org.apache.heron.simulator.utils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Rotating Map maintains a list of unordered maps.
 * Every time a rotate is called, it drops the
 * last map and instantiates a new map at the head
 * of the list. The create operation adds elements
 * to the front map of the list. The anchor and remove
 * operation do their operations starting from the
 * front of the list to the back.
 */

public class RotatingMap {
  private final LinkedList<Map<Long, Long>> buckets = new LinkedList<>();

  // Creates a rotating map with nBuckets maps
  public RotatingMap(int nBuckets) {
    for (int i = 0; i < nBuckets; i++) {
      Map<Long, Long> head = new HashMap<>();
      buckets.addLast(head);
    }
  }

  // Deletes the last map on the list and
  // instantiates a new map at the front of the list
  public void rotate() {
    Map<Long, Long> m = buckets.removeLast();
    buckets.addFirst(new HashMap<Long, Long>());
  }

  // Adds an item to the map at the front of the list
  public void create(long key, long value) {
    buckets.getFirst().put(key, value);
  }

  // Starting from the front of the list,
  // it checks if the key already exists and if so
  // xors another entry to that. Returns true if the
  // xor turns zero. False otherwise
  public boolean anchor(long key, long value) {
    for (Map<Long, Long> m : buckets) {
      if (m.containsKey(key)) {
        long currentValue = m.get(key);
        long newValue = currentValue ^ value;
        m.put(key, newValue);

        return newValue == 0;
      }
    }
    return false;
  }

  // Starting from the front of the list, checks
  // if the key was present in the map and if so
  // removes it. Returns true if the key was removed
  // from some map. False otherwise.
  public boolean remove(long key) {
    for (Map<Long, Long> m : buckets) {
      if (m.remove(key) != null) {
        return true;
      }
    }
    return false;
  }
}
