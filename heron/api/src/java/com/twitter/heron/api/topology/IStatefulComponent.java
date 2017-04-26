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

package com.twitter.heron.api.topology;

import com.twitter.heron.api.state.State;

/**
 * Defines a component that saves its internal state in the State interface
 */
public interface IStatefulComponent extends IComponent {

  /**
   * Initializes the state of the function or operator to that of a previous checkpoint.
   * This method is invoked when a component is executed as part of a recovery run. In case
   * there was prior state associated with the component, the state will be empty.
   * Stateful Spouts/Bolts are expected to hold on to the state variable to save their
   * internal state
   * <p>
   * Note that initialState() is called before open() or prepare().
   *
   * @param state the previously saved state of the component.
   */
  void initState(State state);

  /**
   * This is a hook for the component to perform some actions just before the
   * framework saves its state.
   *
   * @param checkpointId the ID of the checkpoint
   */
  void preSave(String checkpointId);
}
