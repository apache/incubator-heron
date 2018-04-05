// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.heron.simulator.grouping;

import java.util.LinkedList;
import java.util.List;

import org.apache.heron.proto.system.HeronTuples;

public class CustomGrouping extends Grouping {
  public CustomGrouping(List<Integer> taskIds) {
    super(taskIds);
  }

  @Override
  public List<Integer> getListToSend(HeronTuples.HeronDataTuple tuple) {
    // Stmgr does not do the custom grouping.
    // That is done by the instance
    return new LinkedList<>();
  }
}
