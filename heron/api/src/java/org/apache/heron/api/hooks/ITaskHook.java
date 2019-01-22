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

package org.apache.heron.api.hooks;

import java.util.Map;

import org.apache.heron.api.hooks.info.BoltAckInfo;
import org.apache.heron.api.hooks.info.BoltExecuteInfo;
import org.apache.heron.api.hooks.info.BoltFailInfo;
import org.apache.heron.api.hooks.info.EmitInfo;
import org.apache.heron.api.hooks.info.SpoutAckInfo;
import org.apache.heron.api.hooks.info.SpoutFailInfo;
import org.apache.heron.api.topology.TopologyContext;

public interface ITaskHook {
  /**
   * Called after the spout/bolt's open/prepare method is called
   * conf is the Config thats passed to the topology
   */
  void prepare(Map<String, Object> conf, TopologyContext context);

  /**
   * Called just before the spout/bolt's cleanup method is called.
   */
  void cleanup();

  /**
   * Called everytime a tuple is emitted in spout/bolt
   */
  void emit(EmitInfo info);

  /**
   * Called in spout everytime a tuple gets acked
   */
  void spoutAck(SpoutAckInfo info);

  /**
   * Called in spout everytime a tuple gets failed
   */
  void spoutFail(SpoutFailInfo info);

  /**
   * Called in bolt everytime a tuple gets executed
   */
  void boltExecute(BoltExecuteInfo info);

  /**
   * Called in bolt everytime a tuple gets acked
   */
  void boltAck(BoltAckInfo info);

  /**
   * Called in bolt everytime a tuple gets failed
   */
  void boltFail(BoltFailInfo info);
}
