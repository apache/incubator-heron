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

package org.apache.heron.scheduler.yarn;

import org.apache.heron.spi.common.Key;

/**
 * Keys specific to the YARN scheduler
 */
public enum YarnKey {
  // yarn queue for submitting and launching the topology
  HERON_SCHEDULER_YARN_QUEUE("heron.scheduler.yarn.queue", "default"),
  // the amount of memory topology's driver (yarn application master) needs
  YARN_SCHEDULER_DRIVER_MEMORY_MB("heron.scheduler.yarn.driver.memory.mb", 2048);

  private final String value;
  private final Key.Type type;
  private final Object defaultValue;

  YarnKey(String value, String defaultValue) {
    this.value = value;
    this.type = Key.Type.STRING;
    this.defaultValue = defaultValue;
  }

  YarnKey(String value, Integer defaultValue) {
    this.value = value;
    this.type = Key.Type.INTEGER;
    this.defaultValue = defaultValue;
  }

  public String value() {
    return value;
  }

  public Object getDefault() {
    return defaultValue;
  }

  public String getDefaultString() {
    if (type != Key.Type.STRING) {
      throw new IllegalAccessError(String.format(
          "Config Key %s is type %s, getDefaultString() not supported", this.name(), this.type));
    }
    return (String) this.defaultValue;
  }

  public int getDefaultInt() {
    if (type != Key.Type.INTEGER) {
      throw new IllegalAccessError(String.format(
          "Config Key %s is type %s, getDefaultInt() not supported", this.name(), this.type));
    }
    return (Integer) this.defaultValue;
  }
}
