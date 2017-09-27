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

package com.twitter.heron.dsl.impl.streamlets;

import java.util.Set;

import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.dsl.SerializableGenerator;
import com.twitter.heron.dsl.impl.BaseStreamlet;
import com.twitter.heron.dsl.impl.sources.GeneratorSource;

/**
 * SupplierStreamlet is a very quick and flexible way of creating a Streamlet
 * from a user supplied Supplier Function. The supplier function is the
 * source of all tuples for this Streamlet.
 */
public class GeneratorStreamlet<R> extends BaseStreamlet<R> {
  private SerializableGenerator<R> generator;

  public GeneratorStreamlet(SerializableGenerator<R> generator) {
    this.generator = generator;
    setNumPartitions(1);
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    if (getName() == null) {
      setName(defaultNameCalculator("generator", stageNames));
    }
    if (stageNames.contains(getName())) {
      throw new RuntimeException("Duplicate Names");
    }
    stageNames.add(getName());
    bldr.setSpout(getName(), new GeneratorSource<R>(generator), getNumPartitions());
    return true;
  }
}
