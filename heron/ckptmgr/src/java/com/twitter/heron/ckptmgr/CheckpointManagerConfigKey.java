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
package com.twitter.heron.ckptmgr;

import java.util.HashMap;
import java.util.Map;

/**
 * Keys to be used in the CheckpointManagerConfigs
 */
public enum CheckpointManagerConfigKey {

  /**
   * The configs for the backend storage
   */
  STORAGE_CONFIG("heron.statefulstorage.config", Type.MAP),

  /**
   * The class name of the backend storage
   */
  STORAGE_CLASSNAME("heron.class.statefulstorage", Type.STRING),

  /**
   * The batch size in bytes for write operation
   */
  WRITE_BATCH_SIZE("heron.ckptmgr.network.write.batch.size.bytes", Type.LONG),

  /**
   * The time for ckptmgr write batch
   */
  WRITE_BATCH_TIME("heron.ckptmgr.network.write.batch.time.ms", Type.LONG),

  /**
   * The batch size in bytes for read operation
   */
  READ_BATCH_SIZE("heron.ckptmgr.network.read.batch.size.bytes", Type.LONG),

  /**
   * The time for ckptmger read batch
   */
  READ_BATCH_TIME("heron.ckptmgr.network.read.batch.time.ms", Type.LONG),

  /**
   * The size for socket send buffer in bytes
   */
  SOCKET_SEND_SIZE("heron.ckptmgr.network.options.socket.send.buffer.size.bytes", Type.INTEGER),

  /**
   * The size for socket receive buffer in bytes
   */
  SOCKET_RECEIVE_SIZE(
      "heron.ckptmgr.network.options.socket.receive.buffer.size.bytes", Type.INTEGER);

  private final String value;
  private final Object defaultValue;
  private final Type type;

  public enum Type {
    INTEGER,
    LONG,
    STRING,
    MAP
  }

  // Map of value -> enum
  private static final Map<String, CheckpointManagerConfigKey> VALUE_MAP;

  static {
    VALUE_MAP = new HashMap<>();
    for (CheckpointManagerConfigKey key : CheckpointManagerConfigKey.values()) {
      VALUE_MAP.put(key.value(), key);
    }
  }

  CheckpointManagerConfigKey(String value, Type type) {
    this.value = value;
    this.type = type;
    this.defaultValue = null;
  }

  CheckpointManagerConfigKey(String value, Integer defaultValue) {
    this.value = value;
    this.type = Type.INTEGER;
    this.defaultValue = defaultValue;
  }

  CheckpointManagerConfigKey(String value, Long defaultValue) {
    this.value = value;
    this.type = Type.LONG;
    this.defaultValue = defaultValue;
  }

  CheckpointManagerConfigKey(String value, String defaultValue) {
    this.value = value;
    this.type = Type.STRING;
    this.defaultValue = defaultValue;
  }

  CheckpointManagerConfigKey(String value, Map<String, Object> defaultValue) {
    this.value = value;
    this.type = Type.MAP;
    this.defaultValue = defaultValue;
  }

  static CheckpointManagerConfigKey toCheckpointManagerConfigKey(String value) {
    return VALUE_MAP.get(value);
  }

  /**
   * Get the key value for this enum (i.e., heron.directory.home)
   *
   * @return key value
   */
  public String value() {
    return value;
  }

  public Type getType() {
    return type;
  }

  /**
   * Return the default value
   */
  public Object getDefault() {
    return this.defaultValue;
  }

  public String getDefaultString() {
    if (type != Type.STRING) {
      throw new IllegalAccessError(String.format(
          "Config Key %s is type %s, getDefaultString() not supported", this.name(), this.type));
    }
    return (String) this.defaultValue;
  }
}
