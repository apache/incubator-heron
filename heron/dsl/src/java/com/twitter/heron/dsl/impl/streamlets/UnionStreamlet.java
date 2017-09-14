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
import com.twitter.heron.dsl.impl.BaseStreamlet;
import com.twitter.heron.dsl.impl.operators.UnionOperator;

/**
 * UnionStreamlet is a Streamlet composed of all the elements of two
 * parent streamlets.
 */
public class UnionStreamlet<I> extends BaseStreamlet<I> {
  private BaseStreamlet<I> left;
  private BaseStreamlet<? extends I> right;

  public UnionStreamlet(BaseStreamlet<I> left, BaseStreamlet<? extends I> right) {
    this.left = left;
    this.right = right;
    setNumPartitions(left.getNumPartitions());
  }

  private void calculateName(Set<String> stageNames) {
    int index = 1;
    String name;
    while (true) {
      name = new StringBuilder("union").append(index).toString();
      if (!stageNames.contains(name)) {
        break;
      }
      index++;
    }
    setName(name);
  }

  @Override
  public boolean doBuild(TopologyBuilder bldr, Set<String> stageNames) {
    if (!left.isBuilt() || !right.isBuilt()) {
      // We can only continue to build us if both of our parents are built.
      // The system will call us again later
      return false;
    }
    if (getName() == null) {
      calculateName(stageNames);
    }
    if (stageNames.contains(getName())) {
      throw new RuntimeException("Duplicate Names");
    }
    stageNames.add(getName());
    bldr.setBolt(getName(), new UnionOperator<I>(),
        getNumPartitions()).shuffleGrouping(left.getName()).shuffleGrouping(right.getName());
    return true;
  }
}
