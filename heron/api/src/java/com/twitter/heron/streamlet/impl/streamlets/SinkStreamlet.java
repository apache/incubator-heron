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
import com.twitter.heron.streamlet.Sink;
import com.twitter.heron.streamlet.impl.StreamletImpl;
import com.twitter.heron.streamlet.impl.sinks.ComplexSink;

/**
 * SinkStreamlet represents en empty Streamlet that is made up of elements from the parent
 * streamlet after consuming every element. Since elements of the parents are just consumed
 * by the user passed consumer function, nothing is emitted, thus this streamlet is empty.
 */
public class SinkStreamlet<R> extends StreamletImpl<R> {
  private StreamletImpl<R> parent;
  private Sink<R> sink;
  private static final String NAMEPREFIX = "sink";

  public SinkStreamlet(StreamletImpl<R> parent, Sink<R> sink) {
    this.parent = parent;
    this.sink = sink;
    setNumPartitions(parent.getNumPartitions());
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    setDefaultNameIfNone(StreamletNamePrefixes.SINK.toString(), stageNames);
    stageNames.add(getName());
    bldr.setBolt(getName(), new ComplexSink<>(sink),
        getNumPartitions()).shuffleGrouping(parent.getName());
    return true;
  }
}
