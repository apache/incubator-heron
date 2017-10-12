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

package com.twitter.heron.streamlet;

import java.io.Serializable;
import java.util.Map;

import com.twitter.heron.api.state.State;

/**
 * Context is the information available at runtime for operators like transform.
 * It contains basic things like config, runtime information like task,
 * the stream that it is operating on, ProcessState, etc.
 */
public interface Context {
  /**
   * Fetches the task id of the current instance of the operator
   * @return the task id.
   */
  int getTaskId();

  /**
   * Fetches the config of the computation
   * @return config
   */
  Map<String, Object> getConfig();

  /**
   * The stream name that we are operating on
   * @return the stream name that we are operating on
   */
  String getStreamName();

  /**
   * The partition number that we are operating on
   * @return the partition number
   */
  int getStreamPartition();

  /**
   * The state where components can store any of their local state
   * @return The state interface where users can store their local state
   */
  State<Serializable, Serializable> getState();
}
