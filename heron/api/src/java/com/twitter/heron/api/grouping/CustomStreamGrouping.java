// Copyright 2017 Twitter. All rights reserved.
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

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.twitter.heron.api.grouping;

import java.io.Serializable;
import java.util.List;

import com.twitter.heron.api.topology.TopologyContext;

public interface CustomStreamGrouping extends Serializable {

  /**
   * Tells the stream grouping at runtime the tasks in the target bolt.
   * This information should be used in chooseTasks to determine the target tasks.
   * <p>
   * It also tells the grouping the metadata on the stream this grouping will be used on.
   */
  void prepare(
      TopologyContext context,
      String component,
      String streamId,
      List<Integer> targetTasks);

  /**
   * This function implements a custom stream grouping. It takes in as input
   * the number of tasks in the target bolt in prepare and returns the
   * tasks to send the tuples to.
   *
   * @param values the values to group on
   */
  List<Integer> chooseTasks(List<Object> values);
}
