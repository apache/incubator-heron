/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.apache.heron.api.bolt;

import java.io.Serializable;

import org.apache.heron.api.state.HashMapState;
import org.apache.heron.api.state.State;

@SuppressWarnings({"rawtypes", "unchecked", "HiddenField"})
public class StatefulWindowedBoltExecutor<K extends Serializable, V extends Serializable>
    extends WindowedBoltExecutor {
  private static final long serialVersionUID = 4975915597473064341L;
  private final IStatefulWindowedBolt statefulWindowedBolt;
  private State state;

  private static final String USER_STATE = "user.state";

  public StatefulWindowedBoltExecutor(IStatefulWindowedBolt bolt) {
    super(bolt);
    this.statefulWindowedBolt = bolt;
  }

  /**
   * initalize state that is partitioned by window internal and user defined
   * @param state
   */
  @Override
  public void initState(State state) {
    this.state = state;
    // initalize internal windowing state
    super.initState(this.state);
    // initialize user defined state
    if (!this.state.containsKey(USER_STATE)) {
      this.state.put(USER_STATE, new HashMapState<K, V>());
    }
    this.statefulWindowedBolt.initState((State<K, V>) this.state.get(USER_STATE));
  }

  @Override
  public void preSave(String checkpointId) {
    this.statefulWindowedBolt.preSave(checkpointId);
    super.preSave(checkpointId);
  }
}
