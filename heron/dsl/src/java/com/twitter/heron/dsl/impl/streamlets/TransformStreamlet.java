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
import com.twitter.heron.dsl.TransformFunction;
import com.twitter.heron.dsl.impl.BaseStreamlet;
import com.twitter.heron.dsl.impl.operators.TransformOperator;

/**
 * TransformStreamlet represents a Streamlet that is made up of applying the user
 * supplied transform function to each element of the parent streamlet. It differs
 * from the simple MapStreamlet in the sense that it provides setup/cleanup flexibility
 * for the users to setup things and cleanup before the beginning of the computation
 */
public class TransformStreamlet<R, T> extends BaseStreamlet<T> {
  private BaseStreamlet<R> parent;
  private TransformFunction<? super R, ? extends T> transformFunction;

  public TransformStreamlet(BaseStreamlet<R> parent,
                            TransformFunction<? super R, ? extends T> transformFunction) {
    this.parent = parent;
    this.transformFunction = transformFunction;
    setNumPartitions(parent.getNumPartitions());
  }

  private void calculateName(Set<String> stageNames) {
    int index = 1;
    String name;
    while (true) {
      name = new StringBuilder("transform").append(index).toString();
      if (!stageNames.contains(name)) {
        break;
      }
      index++;
    }
    setName(name);
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    if (getName() == null) {
      calculateName(stageNames);
    }
    if (stageNames.contains(getName())) {
      throw new RuntimeException("Duplicate Names");
    }
    stageNames.add(getName());
    bldr.setBolt(getName(), new TransformOperator<R, T>(transformFunction),
        getNumPartitions()).shuffleGrouping(parent.getName());
    return true;
  }
}
