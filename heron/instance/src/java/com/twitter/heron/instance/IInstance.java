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

package com.twitter.heron.instance;

import java.io.Serializable;

import com.google.protobuf.Message;

import com.twitter.heron.api.state.State;
import com.twitter.heron.classification.InterfaceAudience;
import com.twitter.heron.classification.InterfaceStability;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;

/**
 * Implementing this interface allows an object to be target of HeronInstance
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface IInstance {

  /**
   * Initialize the instance. If it's a stateful topology,
   * the provided state will be used for initialization.
   * For non-stateful topology, the state will be ignored.
   * @param state used for stateful topology to initialize the instance state
   */
  void init(State<Serializable, Serializable> state);

  /**
   * Start the execution of the IInstance
   */
  void start();

  /**
   * Clean the instance. After it's called, the IInstance
   * will be still alive but with an empty state. Before
   * starting the IInstance again, an `init()` call is needed
   * to initialize the instance properly.
   */
  void clean();

  /**
   * Destroy the whole IInstance.
   * Notice: It should only be called when the whole program is
   * exiting. And in fact, this method should never be called.
   */
  void shutdown();

  /**
   * Read tuples from a queue and process the tuples
   *
   * @param inQueue the queue to read tuples from
   */
  void readTuplesAndExecute(Communicator<Message> inQueue);

  /**
   * Activate the instance
   */
  void activate();

  /**
   * Deactivate the instance
   */
  void deactivate();

  /**
   * Update the instance. This happens when the physical plan changes (e.g., during a scaling event)
   * @param physicalPlanHelper
   */
  void update(PhysicalPlanHelper physicalPlanHelper);

  /**
   * Save the state and send it out for persistence
   * @param checkpointId
   */
  void persistState(String checkpointId);
}
