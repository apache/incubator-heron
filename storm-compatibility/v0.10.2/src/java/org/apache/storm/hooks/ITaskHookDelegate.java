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

package org.apache.storm.hooks;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.heron.common.basics.TypeUtils;
import org.apache.storm.Config;
import org.apache.storm.hooks.info.BoltAckInfo;
import org.apache.storm.hooks.info.BoltExecuteInfo;
import org.apache.storm.hooks.info.BoltFailInfo;
import org.apache.storm.hooks.info.EmitInfo;
import org.apache.storm.hooks.info.SpoutAckInfo;
import org.apache.storm.hooks.info.SpoutFailInfo;
import org.apache.storm.task.TopologyContext;

/**
 * There would be types of task hooks inside ITaskHookDelegate:
 * 1. task hook's classes specified in config statically.
 * The task hooks' objects would be instantiated by using reflection and added into
 * the list of ITaskHook when the method
 * prepare(Map conf, org.apache.heron.api.topology.TopologyContext context)
 * is invoked.
 * 2. task hook added dynamically by invoking addHook(ITaskHook)
 */
@SuppressWarnings("rawtypes")
public class ITaskHookDelegate implements org.apache.heron.api.hooks.ITaskHook {
  private List<ITaskHook> hooks;
  private Map conf;

  // zero arg constructor
  public ITaskHookDelegate() {
    hooks = new LinkedList<ITaskHook>();
  }

  public void addHook(ITaskHook hook) {
    hooks.add(hook);
  }

  public List<ITaskHook> getHooks() {
    return hooks;
  }

  public Map getConf() {
    return conf;
  }

  @Override
  public void prepare(Map<String, Object> newConf,
                      org.apache.heron.api.topology.TopologyContext context) {
    this.conf = newConf;
    if (!newConf.containsKey(Config.STORMCOMPAT_TOPOLOGY_AUTO_TASK_HOOKS)) {
      throw new RuntimeException("StormCompat Translation not done for task hooks");
    }
    List<String> hookClassNames =
        TypeUtils.getListOfStrings(newConf.get(Config.STORMCOMPAT_TOPOLOGY_AUTO_TASK_HOOKS));

    for (String className : hookClassNames) {
      ITaskHook hook;
      try {
        hook = (ITaskHook) Class.forName(className).newInstance();
      } catch (ClassNotFoundException ex) {
        throw new RuntimeException(ex + " ITaskHook class must be in class path.");
      } catch (InstantiationException ex) {
        throw new RuntimeException(ex + " ITaskHook class must be concrete.");
      } catch (IllegalAccessException ex) {
        throw new RuntimeException(ex + " ITaskHook class must have a no-arg constructor.");
      }

      hooks.add(hook);
    }

    // Invoke the prepare() for all ITaskHooks
    TopologyContext ctxt = new TopologyContext(context);
    for (ITaskHook hook : hooks) {
      hook.prepare(newConf, ctxt);
    }
  }

  @Override
  public void cleanup() {
    for (ITaskHook hook : hooks) {
      hook.cleanup();
    }
  }

  @Override
  public void emit(org.apache.heron.api.hooks.info.EmitInfo info) {
    EmitInfo emit = new EmitInfo(info);
    for (ITaskHook hook : hooks) {
      hook.emit(emit);
    }
  }

  @Override
  public void spoutAck(org.apache.heron.api.hooks.info.SpoutAckInfo info) {
    SpoutAckInfo ack = new SpoutAckInfo(info);
    for (ITaskHook hook : hooks) {
      hook.spoutAck(ack);
    }
  }

  @Override
  public void spoutFail(org.apache.heron.api.hooks.info.SpoutFailInfo info) {
    SpoutFailInfo fail = new SpoutFailInfo(info);
    for (ITaskHook hook : hooks) {
      hook.spoutFail(fail);
    }
  }

  @Override
  public void boltAck(org.apache.heron.api.hooks.info.BoltAckInfo info) {
    BoltAckInfo ack = new BoltAckInfo(info);
    for (ITaskHook hook : hooks) {
      hook.boltAck(ack);
    }
  }

  @Override
  public void boltFail(org.apache.heron.api.hooks.info.BoltFailInfo info) {
    BoltFailInfo fail = new BoltFailInfo(info);
    for (ITaskHook hook : hooks) {
      hook.boltFail(fail);
    }
  }

  @Override
  public void boltExecute(org.apache.heron.api.hooks.info.BoltExecuteInfo info) {
    BoltExecuteInfo execute = new BoltExecuteInfo(info);
    for (ITaskHook hook : hooks) {
      hook.boltExecute(execute);
    }
  }
}
