//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package com.twitter.heron.dsl.impl.spouts;

import java.util.Map;
import java.util.function.Supplier;

import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Values;

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
public class SupplierSpout<R> extends DslSpout {
  private static final long serialVersionUID = 6476611751545430216L;
  private Supplier<R> supplier;

  private SpoutOutputCollector collector;

  public SupplierSpout(Supplier<R> supplier) {
    this.supplier = supplier;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector outputCollector) {
    collector = outputCollector;
  }

  @Override
  public void nextTuple() {
    collector.emit(new Values(supplier.get()));
  }
}
