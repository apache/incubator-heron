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

package org.apache.storm.topology;

import java.util.Map;

public class SpoutDeclarerImpl implements SpoutDeclarer {
  private com.twitter.heron.api.topology.SpoutDeclarer delegate;

  public SpoutDeclarerImpl(com.twitter.heron.api.topology.SpoutDeclarer delegate) {
    this.delegate = delegate;
  }

  @Override
  public SpoutDeclarer addConfigurations(Map<String, Object> conf) {
    delegate.addConfigurations(conf);
    return this;
  }

  @Override
  public SpoutDeclarer addConfiguration(String config, Object value) {
    delegate.addConfiguration(config, value);
    return this;
  }

  @Override
  public SpoutDeclarer setDebug(boolean debug) {
    delegate.setDebug(debug);
    return this;
  }

  @Override
  public SpoutDeclarer setMaxTaskParallelism(Number val) {
    // Heron does not support this
    return this;
  }

  @Override
  public SpoutDeclarer setMaxSpoutPending(Number val) {
    delegate.setMaxSpoutPending(val);
    return this;
  }

  @Override
  public SpoutDeclarer setNumTasks(Number val) {
    // Heron does not support this
    return this;
  }
}
