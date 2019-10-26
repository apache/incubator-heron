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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.heron.common.basics.WakeableLooper;

public class XORManager {
  private final WakeableLooper looper;

  // map of task_id to a RotatingMap
  private final Map<Integer, RotatingMap> spoutTasksToRotatingMap;

  // The rotate interval in nano-seconds
  private final Duration rotateInterval;

  /**
   * Get an XORManager for all spouts for the topology.
   *
   * @param looper The WakeableLooper to execute timer event
   * @param topologyManager The manager which contains a topology protobuf
   * @param nBuckets number of buckets to divide the message timeout seconds
   */
  public XORManager(WakeableLooper looper, TopologyManager topologyManager, int nBuckets) {
    this.looper = looper;
    this.spoutTasksToRotatingMap = new HashMap<>();

    for (Integer taskId : topologyManager.getSpoutTasks()) {
      spoutTasksToRotatingMap.put(taskId, new RotatingMap(nBuckets));
    }

    Duration timeout = topologyManager.extractTopologyTimeout();

    // TODO Is this correct?
    this.rotateInterval = timeout.dividedBy(nBuckets).plusNanos(timeout.getNano());

    looper.registerTimerEvent(timeout, this::rotate);
  }

  // Create a new entry for the tuple.
  // taskId is the task id where the tuple
  // originated from.
  // key is generated in spout for rootId
  // value is the tuple key as seen by the
  // destination
  public void create(int taskId, long key, long value) {
    spoutTasksToRotatingMap.get(taskId).create(key, value);
  }

  // Add one more entry to the tuple tree
  // taskId is the task id where the tuple
  // originated from.
  // key is generated in spout for rootId
  // value is the tuple key as seen by the
  // destination
  // We return true if the xor value is now zerod out
  // Else return false
  public boolean anchor(int taskId, long key, long value) {
    return spoutTasksToRotatingMap.get(taskId).anchor(key, value);
  }

  // remove this tuple key from our structure.
  // return true if this key was found. else false
  public boolean remove(int taskId, long key) {
    return spoutTasksToRotatingMap.get(taskId).remove(key);
  }

  // Invoke the rotate methods for all RotatingMap of all spout tasks
  // Protected method for unit test
  protected void rotate() {
    for (RotatingMap map : spoutTasksToRotatingMap.values()) {
      map.rotate();
    }

    Runnable r = new Runnable() {
      @Override
      public void run() {
        rotate();
      }
    };
    looper.registerTimerEvent(rotateInterval, r);
  }

  // For unit test
  protected Map<Integer, RotatingMap> getSpoutTasksToRotatingMap() {
    return spoutTasksToRotatingMap;
  }
}
