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

package com.twitter.heron.spi.scheduler;

import com.twitter.heron.classification.InterfaceAudience;
import com.twitter.heron.classification.InterfaceStability;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.packing.PackingPlan;

/**
 * Launches scheduler. heron-cli will create Launcher object using default no argument constructor.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Evolving
public interface ILauncher extends AutoCloseable {
  /**
   * Initialize Launcher with Config, Uploader and topology. These object
   * will be passed from submitter main. Config will contain information that launcher may use
   * to setup scheduler and other parameters required by launcher to contact
   * services which will launch scheduler.
   */
  void initialize(Config config, Config runtime);

  /**
   * This is to for disposing or cleaning up any internal state accumulated by
   * the ILauncher
   * <p>
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   */
  void close();

  /**
   * Starts scheduler. Once this function returns successfully, heron-cli will terminate and
   * the launch process succeeded.
   *
   * @param packing Initial mapping suggested by running packing algorithm.
   * container_id-&gt;List of instance_id to be launched on this container.
   * @return true if topology launched successfully, false otherwise.
   */
  boolean launch(PackingPlan packing);
}
