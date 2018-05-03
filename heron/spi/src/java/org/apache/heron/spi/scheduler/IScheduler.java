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

package org.apache.heron.spi.scheduler;

import java.util.List;

import org.apache.heron.classification.InterfaceAudience;
import org.apache.heron.classification.InterfaceStability;
import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.packing.PackingPlan;

/**
 * Scheduler object responsible for bringing up topology. Will be instantiated using no-arg
 * constructor.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Evolving
public interface IScheduler extends AutoCloseable {
  /**
   * This will initialize scheduler using config file. Will be called during start.
   */
  void initialize(Config config, Config runtime);

  /**
   * This is to for disposing or cleaning up any internal state accumulated by
   * the scheduler
   * <p>
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   */
  void close();

  /**
   * This method will be called after initialize.
   * It is responsible for grabbing resource to launch executor and make sure they
   * get launched.
   * <p>
   *
   * @param packing Initial mapping suggested by running packing algorithm.
   */
  boolean onSchedule(PackingPlan packing);

  /**
   * This method will be called after onScheduler
   * It is responsible to return links to topology's customized ui pages.
   * Example: link to the Mesos Slave UI page displaying all scheduled containers
   *
   * @return the links if any customized page. It returns an empty list if no any links
   */
  List<String> getJobLinks();

  /**
   * Called by SchedulerServer when it receives a http request to kill topology,
   * while the http request body would be the protobuf Scheduler.KillTopologyRequest.
   * The SchedulerServer would parse the request body and feed with this method.
   * It would be invoked in the executors of SchedulerServer.
   *
   * @param request The KillTopologyRequest sent from local heron-cli
   * @return true if the IScheduler kills the topology successfully. SchedulerServer would
   * send KillTopologyResponse correspondingly according to this method's return value.
   */
  boolean onKill(Scheduler.KillTopologyRequest request);

  /**
   * Called by SchedulerServer when it receives a http request to restart topology,
   * while the http request body would be the protobuf Scheduler.RestartTopologyRequest.
   * The SchedulerServer would parse the request body and feed with this method.
   * It would be invoked in the executors of SchedulerServer.
   *
   * @param request The RestartTopologyRequest sent from local heron-cli
   * @return true if the IScheduler restarts the topology successfully. SchedulerServer would
   * send RestartTopologyResponse correspondingly according to this method's return value.
   */
  boolean onRestart(Scheduler.RestartTopologyRequest request);

  /**
   * Called by SchedulerServer when it receives a http request to update topology,
   * while the http request body would be the protobuf Scheduler.UpdateTopologyRequest.
   * The SchedulerServer would parse the request body and feed with this method.
   * It would be invoked in the executors of SchedulerServer.
   *
   * @param request The UpdateTopologyRequest sent from local heron-cli
   * @return true if the IScheduler updates the topology successfully. SchedulerServer would
   * send UpdateTopologyResponse correspondingly according to this method's return value.
   */
  boolean onUpdate(Scheduler.UpdateTopologyRequest request);
}
