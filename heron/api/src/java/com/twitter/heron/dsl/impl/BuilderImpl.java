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
import com.twitter.heron.dsl.KVStreamlet;
import com.twitter.heron.dsl.KeyValue;
import com.twitter.heron.dsl.SerializableGenerator;
import com.twitter.heron.dsl.SerializableSupplier;
import com.twitter.heron.dsl.Streamlet;

/**
 * BuilderImpl implements the Builder interface.
 * The builder keeps track of the all the sources of the
 * computation dag and builds the Topology by traversing all
 * the computation nodes.
 */
public final class BuilderImpl implements Builder {
  private List<BaseStreamlet<?>> sources;
  public BuilderImpl() {
    sources = new LinkedList<>();
  }

  @Override
  public <R> Streamlet<R> newSource(SerializableSupplier<R> supplier) {
    BaseStreamlet<R> retval = BaseStreamlet.createSupplierStreamlet(supplier);
    retval.setNumPartitions(1);
    sources.add(retval);
    return retval;
  }

  @Override
  public <K, V> KVStreamlet<K, V> newKVSource(SerializableSupplier<KeyValue<K, V>> supplier) {
    BaseKVStreamlet<K, V> retval = BaseKVStreamlet.createSupplierKVStreamlet(supplier);
    retval.setNumPartitions(1);
    sources.add(retval);
    return retval;
  }

  @Override
  public <R> Streamlet<R> newSource(SerializableGenerator<R> generator) {
    BaseStreamlet<R> retval = BaseStreamlet.createGeneratorStreamlet(generator);
    retval.setNumPartitions(1);
    sources.add(retval);
    return retval;
  }

  @Override
  public <K, V> KVStreamlet<K, V> newKVSource(SerializableGenerator<KeyValue<K, V>> generator) {
    BaseKVStreamlet<K, V> retval = BaseKVStreamlet.createGeneratorKVStreamlet(generator);
    retval.setNumPartitions(1);
    sources.add(retval);
    return retval;
  }

  /**
   * We start traversing from all sources and build each node.
   * @return TopologyBuilder class that represents the built topology
   */
  public TopologyBuilder build() {
    TopologyBuilder builder = new TopologyBuilder();
    Set<String> stageNames = new HashSet<>();
    for (BaseStreamlet<?> streamlet : sources) {
      streamlet.build(builder, stageNames);
    }
    for (BaseStreamlet<?> streamlet : sources) {
      if (!streamlet.allBuilt()) {
        throw new RuntimeException("Topology cannot be fully built! Are all sources added?");
      }
    }
    return builder;
  }
}
