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

package com.twitter.heron.streamlet.impl.streamlets;

import java.util.Set;

import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.streamlet.SerializableConsumer;
import com.twitter.heron.streamlet.impl.BaseStreamlet;
import com.twitter.heron.streamlet.impl.sinks.ConsumerSink;

/**
 * ConsumerStreamlet represents en empty Streamlet that is made up of elements from the parent
 * streamlet after consuming every element. Since elements of the parents are just consumed
 * by the user passed consumer function, nothing is emitted, thus this streamlet is empty.
 */
public class ConsumerStreamlet<R> extends BaseStreamlet<R> {
  private BaseStreamlet<R> parent;
  private SerializableConsumer<R> consumer;

  public ConsumerStreamlet(BaseStreamlet<R> parent, SerializableConsumer<R> consumer) {
    this.parent = parent;
    this.consumer = consumer;
    setNumPartitions(parent.getNumPartitions());
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    if (getName() == null) {
      setName(defaultNameCalculator("consumer", stageNames));
    }
    if (stageNames.contains(getName())) {
      throw new RuntimeException("Duplicate Names");
    }
    stageNames.add(getName());
    bldr.setBolt(getName(), new ConsumerSink<>(consumer),
        getNumPartitions()).shuffleGrouping(parent.getName());
    return true;
  }
}
