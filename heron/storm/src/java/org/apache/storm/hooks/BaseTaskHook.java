/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.hooks;

import java.util.Map;

import org.apache.storm.hooks.info.BoltAckInfo;
import org.apache.storm.hooks.info.BoltExecuteInfo;
import org.apache.storm.hooks.info.BoltFailInfo;
import org.apache.storm.hooks.info.EmitInfo;
import org.apache.storm.hooks.info.SpoutAckInfo;
import org.apache.storm.hooks.info.SpoutFailInfo;
import org.apache.storm.task.TopologyContext;

public class BaseTaskHook implements ITaskHook {
  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(Map conf, TopologyContext context) {
  }

  @Override
  public void cleanup() {
  }

  @Override
  public void emit(EmitInfo info) {
  }

  @Override
  public void spoutAck(SpoutAckInfo info) {
  }

  @Override
  public void spoutFail(SpoutFailInfo info) {
  }

  @Override
  public void boltAck(BoltAckInfo info) {
  }

  @Override
  public void boltFail(BoltFailInfo info) {
  }

  @Override
  public void boltExecute(BoltExecuteInfo info) {
  }
}
