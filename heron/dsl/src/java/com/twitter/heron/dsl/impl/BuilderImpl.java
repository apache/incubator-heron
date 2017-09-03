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

package com.twitter.heron.dsl.impl;


import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.dsl.Builder;
import com.twitter.heron.dsl.Streamlet;
import com.twitter.heron.dsl.impl.streamlets.StreamletImpl;

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
public final class BuilderImpl implements Builder {
  private List<StreamletImpl<?>> sources;
  public BuilderImpl() {
    sources = new LinkedList<>();
  }
  public <R> Streamlet<R> addSource(Streamlet<R> newSource) {
    StreamletImpl<R> source = (StreamletImpl<R>) newSource;
    sources.add(source);
    return newSource;
  }
  public TopologyBuilder build() {
    TopologyBuilder builder = new TopologyBuilder();
    Set<String> stageNames = new HashSet<>();
    for (StreamletImpl<?> streamlet : sources) {
      streamlet.build(builder, stageNames);
    }
    return builder;
  }
}
