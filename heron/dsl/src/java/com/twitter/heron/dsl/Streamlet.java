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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  protected String stageName;
  protected int nPartitions;
  public Streamlet<R> setName(String stageName) {
    this.stageName = stageName;
    return this;
  }
  public Streamlet<R> setNumPartitions(int nPartitions) {
    this.nPartitions = nPartitions;
    return this;
  }

  <T> Streamlet<T> map(Function<R, T> mapFn) {
    new MapStreamlet(mapFn);
  }

  <T> Streamlet<T> flatMap(Function<R, Collection<T>> flatMapFn) {
    new FlatMapStreamlet(flatMapFn);
  }

  Streamlet<R> filter(Predicate<R> filterFn) {
    return FilterStreamlet(filterFn);
  }

  Streamlet<R> repartition(int nPartitions) {
    return RepartitionStreamlet(nPartitions);
  }
  protected Streamlet() {
    this.stageName = "NotAssigned";
    this.nPartitions = 1;
  }

  protected TopologyBuilder build(TopologyBuilder bldr, Set<String> stageNames);

  public void run() {
  }
}
