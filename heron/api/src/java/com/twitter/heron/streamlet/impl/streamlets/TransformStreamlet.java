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
import com.twitter.heron.streamlet.SerializableTransformer;
import com.twitter.heron.streamlet.impl.StreamletImpl;
import com.twitter.heron.streamlet.impl.operators.TransformOperator;

/**
 * TransformStreamlet represents a Streamlet that is made up of applying the user
 * supplied transform function to each element of the parent streamlet. It differs
 * from the simple MapStreamlet in the sense that it provides setup/cleanup flexibility
 * for the users to setup things and cleanup before the beginning of the computation
 */
public class TransformStreamlet<R, T> extends StreamletImpl<T> {
  private StreamletImpl<R> parent;
  private SerializableTransformer<? super R, ? extends T> serializableTransformer;

  public TransformStreamlet(StreamletImpl<R> parent,
                       SerializableTransformer<? super R, ? extends T> serializableTransformer) {
    this.parent = parent;
    this.serializableTransformer = serializableTransformer;
    setNumPartitions(parent.getNumPartitions());
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    setDefaultNameIfNone(StreamletNamePrefixes.TRANSFORM.toString(), stageNames);
    bldr.setBolt(getName(), new TransformOperator<R, T>(serializableTransformer),
        getNumPartitions()).shuffleGrouping(parent.getName());
    return true;
  }
}
