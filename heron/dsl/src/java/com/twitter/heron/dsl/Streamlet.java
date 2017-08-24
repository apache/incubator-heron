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

package com.twitter.heron.dsl;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.exception.AlreadyAliveException;
import com.twitter.heron.api.exception.InvalidTopologyException;
import com.twitter.heron.api.topology.TopologyBuilder;

/**
 * A Streamlet is a (potentially unbounded) ordered collection of tuples.
 Streamlets originate from pub/sub systems(such Pulsar/Kafka), or from static data(such as
 csv files, HDFS files), or for that matter any other source. They are also created by
 transforming existing Streamlets using operations such as map/flatMap, etc.
 Besides the tuples, a Streamlet has the following properties associated with it
 a) name. User assigned or system generated name to refer the streamlet
 b) nPartitions. Number of partitions that the streamlet is composed of. The nPartitions
 could be assigned by the user or computed by the system
 */
public abstract class Streamlet<R> {
  protected String name;
  protected int nPartitions;
  public Streamlet<R> setName(String sName) {
    if (sName == null) {
      throw new IllegalArgumentException("Streamlet name cannot be null");
    }
    this.name = sName;
    return this;
  }
  public String getName() {
    return name;
  }
  public Streamlet<R> setNumPartitions(int numPartitions) {
    if (numPartitions < 1) {
      throw new IllegalArgumentException("Streamlet's partitions cannot be < 1");
    }
    this.nPartitions = numPartitions;
    return this;
  }
  public int getNumPartitions() {
    return nPartitions;
  }

  <T> Streamlet<T> map(Function<R, T> mapFn) {
    return new MapStreamlet<R, T>(this, mapFn);
  }

  <T> Streamlet<T> flatMap(Function<R, Iterable<T>> flatMapFn) {
    return new FlatMapStreamlet<R, T>(this, flatMapFn);
  }

  /*
  Streamlet<R> filter(Predicate<R> filterFn) {
    return FilterStreamlet(filterFn);
  }

  Streamlet<R> repartition(int nPartitions) {
    return RepartitionStreamlet(nPartitions);
  }
  */

  protected Streamlet() {
    this.nPartitions = -1;
  }

  abstract TopologyBuilder build(TopologyBuilder bldr, Set<String> stageNames);

  public void run(String topologyName, Config config) {
    Set<String> stageNames = new HashSet<>();
    TopologyBuilder bldr = new TopologyBuilder();
    bldr = build(bldr, stageNames);
    try {
      HeronSubmitter.submitTopology(topologyName, config, bldr.createTopology());
    } catch (AlreadyAliveException e) {
      e.printStackTrace();
    } catch (InvalidTopologyException e) {
      e.printStackTrace();
    }
  }
}
