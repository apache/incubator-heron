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

package com.twitter.heron.dsl;

import com.twitter.heron.api.Config;
import com.twitter.heron.dsl.impl.RunnerImpl;

/**
 * Runner is used to run a topology that is built by the builder.
 * It exports a sole function called run that takes care of constructing the topology
 */
public interface Runner {
  static Runner CreateRunner() {
    return new RunnerImpl();
  }

  /**
   * Runs the computation
   * @param name The name of the topology
   * @param config Any config thats passed to the topology
   * @param builder The builder used to keep track of the sources.
   */
  void run(String name, Config config, Builder builder);
}
