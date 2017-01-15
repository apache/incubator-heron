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

package com.twitter.heron.simulator.utils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.Constants;
import com.twitter.heron.common.basics.WakeableLooper;

public class XORManager {
  private final WakeableLooper looper;

  // map of task_id to a RotatingMap
  private final Map<Integer, RotatingMap> spoutTasksToRotatingMap;

  // The rotate interval in nano-seconds
  private final long rotateIntervalNs;

  public XORManager(WakeableLooper looper,
                    int timeoutSec,
                    List<Integer> taskIds,
                    int nBuckets) {
    this.looper = looper;
    this.spoutTasksToRotatingMap = new HashMap<>();

    // We would do the first rotate after timeoutSec seconds,
    // And then would do the rotate at every rotateIntervalNs nano-second
    Runnable r = new Runnable() {
      @Override
      public void run() {
        rotate();
      }
    };
    looper.registerTimerEventInNanoSeconds(timeoutSec * Constants.SECONDS_TO_NANOSECONDS, r);

    this.rotateIntervalNs = Constants.SECONDS_TO_NANOSECONDS * timeoutSec / nBuckets
        + (Constants.SECONDS_TO_NANOSECONDS * timeoutSec) % nBuckets;

    for (Integer taskId : taskIds) {
      spoutTasksToRotatingMap.put(taskId, new RotatingMap(nBuckets));
    }
  }

  /**
   * Populate the XORManager for all spouts for the topology.
   *
   * @param looper The WakeableLooper to execute timer event
   * @param topology The given topology protobuf
   * @param nBuckets number of buckets to divide the message timeout seconds
   * @param componentToTaskIds the map of componentName to its list of taskIds in the topology
   * @return the XORManager for all spouts' task for the topology
   */
  public static XORManager populateXORManager(WakeableLooper looper,
                                              TopologyAPI.Topology topology,
                                              int nBuckets,
                                              Map<String, List<Integer>> componentToTaskIds) {
    List<Integer> allSpoutTasks = new LinkedList<>();

    // Only spouts need acking management, i.e. xor maintenance
    for (TopologyAPI.Spout spout : topology.getSpoutsList()) {
      for (TopologyAPI.OutputStream outputStream : spout.getOutputsList()) {
        List<Integer> spoutTaskIds =
            componentToTaskIds.get(outputStream.getStream().getComponentName());
        allSpoutTasks.addAll(spoutTaskIds);
      }
    }

    return new XORManager(looper,
        PhysicalPlanUtil.extractTopologyTimeout(topology),
        allSpoutTasks,
        nBuckets);
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

    // Plan itself in rotateIntervalNs interval
    Runnable r = new Runnable() {
      @Override
      public void run() {
        rotate();
      }
    };
    looper.registerTimerEventInNanoSeconds(rotateIntervalNs, r);
  }

  // For unit test
  protected Map<Integer, RotatingMap> getSpoutTasksToRotatingMap() {
    return spoutTasksToRotatingMap;
  }
}
