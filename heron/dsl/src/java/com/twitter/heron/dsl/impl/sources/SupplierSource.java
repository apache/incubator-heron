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

package com.twitter.heron.dsl.impl.sources;

import java.util.Map;

import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.dsl.SerializableSupplier;

/**
 * SupplierSource is a way to wrap a supplier function inside a Heron Spout.
 * The SupplierSource just calls the get method of the supplied function
 * to generate the next tuple.
 */
public class SupplierSource<R> extends DslSource {
  private static final long serialVersionUID = 6476611751545430216L;
  private SerializableSupplier<R> supplier;

  private SpoutOutputCollector collector;

  public SupplierSource(SerializableSupplier<R> supplier) {
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
