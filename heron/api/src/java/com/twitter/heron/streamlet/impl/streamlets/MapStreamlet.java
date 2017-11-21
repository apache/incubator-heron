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
import com.twitter.heron.streamlet.SerializableFunction;
import com.twitter.heron.streamlet.impl.StreamletImpl;
import com.twitter.heron.streamlet.impl.operators.MapOperator;

/**
 * MapStreamlet represents a Streamlet that is made up of applying the user
 * supplied map function to each element of the parent streamlet.
 */
public class MapStreamlet<R, T> extends StreamletImpl<T> {
  private StreamletImpl<R> parent;
  private SerializableFunction<? super R, ? extends T> mapFn;
  private static final String NAMEPREFIX = "map";

  public MapStreamlet(StreamletImpl<R> parent, SerializableFunction<? super R, ? extends T> mapFn) {
    this.parent = parent;
    this.mapFn = mapFn;
    setNumPartitions(parent.getNumPartitions());
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    setDefaultNameIfNone(StreamletNamePrefixes.MAP.toString(), stageNames);
    stageNames.add(getName());
    bldr.setBolt(getName(), new MapOperator<R, T>(mapFn),
        getNumPartitions()).shuffleGrouping(parent.getName());
    return true;
  }
}
