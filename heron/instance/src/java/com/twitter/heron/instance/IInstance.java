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

import com.twitter.heron.classification.InterfaceAudience;
import com.twitter.heron.classification.InterfaceStability;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.proto.system.HeronTuples;

/**
 * Implementing this interface allows an object to be target of HeronInstance
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface IInstance {
  /**
   * Do the basic setup for HeronInstance
   */
  void start();

  /**
   * Do the basic clean for HeronInstance
   * Notice: this method is not guaranteed to invoke
   * TODO: - to avoid confusing, we in fact have never called this method yet
   * TODO: - need to consider whether or not call this method more carefully
   */
  void stop();

  /**
   * Read tuples from a queue and process the tuples
   *
   * @param inQueue the queue to read tuples from
   */
  void readTuplesAndExecute(Communicator<HeronTuples.HeronTupleSet> inQueue);

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
}
