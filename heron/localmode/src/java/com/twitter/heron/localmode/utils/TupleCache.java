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

package com.twitter.heron.localmode.utils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.HeronTuples;

public class TupleCache {
  private final Map<Integer, TupleList> cache = new HashMap<>();

  protected TupleList get(int destTaskId) {
    TupleList list = cache.get(destTaskId);
    if (list == null) {
      list = new TupleList();
      cache.put(destTaskId, list);
    }

    return list;
  }

  public long addDataTuple(int destTaskId,
                           TopologyAPI.StreamId streamId,
                           HeronTuples.HeronDataTuple tuple,
                           boolean isAnchored) {
    return get(destTaskId).addDataTuple(streamId, tuple, isAnchored);
  }

  public void addAckTuple(int taskId, HeronTuples.AckTuple tuple) {
    get(taskId).addAckTuple(tuple);
  }

  public void addFailTuple(int taskId, HeronTuples.AckTuple tuple) {
    get(taskId).addFailTuple(tuple);
  }

  public void addEmitTuple(int taskId, HeronTuples.AckTuple tuple) {
    get(taskId).addEmitTuple(tuple);
  }

  // Construct a new Map from current cache
  // Modification on Map would not cahnge values in cache
  public Map<Integer, List<HeronTuples.HeronTupleSet>> getCache() {
    Map<Integer, List<HeronTuples.HeronTupleSet>> res =
        new HashMap<>();
    for (Map.Entry<Integer, TupleList> entry : cache.entrySet()) {
      res.put(entry.getKey(), entry.getValue().getTuplesList());
    }

    return res;
  }

  public boolean isEmpty() {
    return cache.isEmpty();
  }

  public void clear() {
    cache.clear();
  }

  protected static class TupleList {
    private final List<HeronTuples.HeronTupleSet> tuples;
    private final Random random;

    private HeronTuples.HeronTupleSet.Builder current;

    public TupleList() {
      tuples = new LinkedList<>();
      random = new Random();
    }

    // returns the tuple key used for XOR
    public long addDataTuple(TopologyAPI.StreamId streamId,
                             HeronTuples.HeronDataTuple tuple,
                             boolean isAnchored) {
      if (current == null ||
          current.hasControl() ||
          !current.getDataBuilder().getStream().getComponentName().equals(streamId.getComponentName()) ||
          !current.getDataBuilder().getStream().getId().equals(streamId.getId())) {
        if (current != null) {
          tuples.add(current.build());
        }
        current = HeronTuples.HeronTupleSet.newBuilder();
        current.getDataBuilder().setStream(streamId);
      }

      long tupleKey = -1;
      if (isAnchored) {
        tupleKey = random.nextLong();

        current.getDataBuilder().addTuples(
            HeronTuples.HeronDataTuple.newBuilder().mergeFrom(tuple).setKey(tupleKey));
      } else {
        // We don't care tuple key value
        current.getDataBuilder().addTuples(tuple);
      }

      return tupleKey;
    }

    public void addAckTuple(HeronTuples.AckTuple tuple) {
      if (current == null ||
          current.hasData() ||
          current.getControlBuilder().getFailsCount() > 0 ||
          current.getControlBuilder().getEmitsCount() > 0) {
        if (current != null) {
          tuples.add(current.build());
        }

        current = HeronTuples.HeronTupleSet.newBuilder();
      }

      current.getControlBuilder().addAcks(tuple);
    }

    public void addFailTuple(HeronTuples.AckTuple tuple) {
      if (current == null ||
          current.hasData() ||
          current.getControlBuilder().getAcksCount() > 0 ||
          current.getControlBuilder().getEmitsCount() > 0) {
        if (current != null) {
          tuples.add(current.build());
        }

        current = HeronTuples.HeronTupleSet.newBuilder();
      }

      current.getControlBuilder().addFails(tuple);
    }

    public void addEmitTuple(HeronTuples.AckTuple tuple) {
      if (current == null ||
          current.hasData() ||
          current.getControlBuilder().getAcksCount() > 0 ||
          current.getControlBuilder().getFailsCount() > 0) {
        if (current != null) {
          tuples.add(current.build());
        }

        current = HeronTuples.HeronTupleSet.newBuilder();
      }

      current.getControlBuilder().addEmits(tuple);
    }

    public List<HeronTuples.HeronTupleSet> getTuplesList() {
      // Add current to tuple list if current is not null
      if (current != null) {
        tuples.add(current.build());

        // Reset current
        current = null;
      }

      return tuples;
    }

    public void clear() {
      current = null;
      tuples.clear();
    }
  }
}
