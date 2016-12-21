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
 * Common methods for all possible components in a topology. This interface is used
 * when defining topologies using the Java API.
 */
public interface IStatefulComponent extends IComponent {

  /**
     * This method is invoked by the framework with the previously
     * saved state of the component. This is invoked after prepare but before
     * the component starts processing tuples.
     *
     * @param state the previously saved state of the component.
     */
  void initState(State state);

  /**
     * This is a hook for the component to perform some actions just before the
     * framework saves its state.
     */
  void preSave();
}
