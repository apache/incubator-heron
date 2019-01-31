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

package org.apache.heron.scheduler.local;

import org.apache.heron.spi.common.Key;

/**
 * Keys specific to the local scheduler
 */
public enum LocalKey {
  // config key for specifying the working directory of a topology
  WORKING_DIRECTORY("heron.scheduler.local.working.directory",
      "${HOME}/.herondata/topologies/${CLUSTER}/${ROLE}/${TOPOLOGY}");

  private final String value;
  private final Key.Type type;
  private final Object defaultValue;

  LocalKey(String value, String defaultValue) {
    this.value = value;
    this.type = Key.Type.STRING;
    this.defaultValue = defaultValue;
  }

  LocalKey(String value, boolean defaultValue) {
    this.value = value;
    this.type = Key.Type.BOOLEAN;
    this.defaultValue = defaultValue;
  }

  public String value() {
    return value;
  }

  public Object getDefault() {
    return defaultValue;
  }

  public Boolean getDefaultBoolean() {
    if (type != Key.Type.BOOLEAN) {
      throw new IllegalAccessError(String.format(
          "Config Key %s is type %s, getDefaultBoolean() not supported", this.name(), this.type));
    }
    return (Boolean) this.defaultValue;
  }

  public String getDefaultString() {
    if (type != Key.Type.STRING) {
      throw new IllegalAccessError(String.format(
          "Config Key %s is type %s, getDefaultString() not supported", this.name(), this.type));
    }
    return (String) this.defaultValue;
  }
}
