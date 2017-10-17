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

package com.twitter.heron.api.windowing;

/**
 * Triggers the window calculations based on the policy.
 *
 * @param <T> the type of the event that is tracked
 */
public interface TriggerPolicy<T> {
  /**
   * Tracks the event and could use this to invoke the trigger.
   *
   * @param event the input event
   */
  void track(Event<T> event);

  /**
   * resets the trigger policy.
   */
  void reset();

  /**
   * Starts the trigger policy. This can be used
   * during recovery to start the triggers after
   * recovery is complete.
   */
  void start();

  /**
   * Any clean up could be handled here.
   */
  void shutdown();
}
