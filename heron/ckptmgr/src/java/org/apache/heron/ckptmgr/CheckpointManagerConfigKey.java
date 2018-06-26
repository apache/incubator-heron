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

package org.apache.heron.ckptmgr;

import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.HashMap;
import java.util.Map;

/**
 * Keys to be used in the CheckpointManagerConfigs
 */
public enum CheckpointManagerConfigKey {

  /**
   * The configs for the stateful storage
   */
  STORAGE_CONFIG("heron.statefulstorage.config", Type.MAP),

  /**
   * The class name of the stateful storage
   */
  STORAGE_CLASSNAME("heron.statefulstorage.classname", Type.STRING),

  /**
   * The batch size in bytes for write operation
   */
  WRITE_BATCH_SIZE("heron.ckptmgr.network.write.batch.size.bytes", Type.BYTE_AMOUNT),

  /**
   * The time for ckptmgr write batch
   */
  WRITE_BATCH_TIME("heron.ckptmgr.network.write.batch.time.ms", ChronoUnit.MILLIS),

  /**
   * The batch size in bytes for read operation
   */
  READ_BATCH_SIZE("heron.ckptmgr.network.read.batch.size.bytes", Type.BYTE_AMOUNT),

  /**
   * The time for ckptmger read batch
   */
  READ_BATCH_TIME("heron.ckptmgr.network.read.batch.time.ms", ChronoUnit.MILLIS),

  /**
   * The size for socket send buffer in bytes
   */
  SOCKET_SEND_SIZE("heron.ckptmgr.network.options.socket.send.buffer.size.bytes", Type.BYTE_AMOUNT),

  /**
   * The size for socket receive buffer in bytes
   */
  SOCKET_RECEIVE_SIZE(
      "heron.ckptmgr.network.options.socket.receive.buffer.size.bytes", Type.BYTE_AMOUNT),

  /**
   * The maximum size of a packet that can be read by the checkpoint manager
   */
  MAXIMUM_PACKET_SIZE(
      "heron.ckptmgr.network.options.maximum.packetsize.bytes", Type.BYTE_AMOUNT);

  private final String value;
  private final Object defaultValue;
  private final Type type;
  private final TemporalUnit temporalUnit;

  public enum Type {
    BYTE_AMOUNT,
    DURATION,
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
    if (type == Type.DURATION) {
      throw new IllegalArgumentException("DURATION types are created by passing a temporalUnit");
    }
    this.value = value;
    this.type = type;
    this.defaultValue = null;
    this.temporalUnit = null;
  }

  CheckpointManagerConfigKey(String value, Integer defaultValue) {
    this.value = value;
    this.type = Type.INTEGER;
    this.defaultValue = defaultValue;
    this.temporalUnit = null;
  }

  CheckpointManagerConfigKey(String value, Long defaultValue) {
    this.value = value;
    this.type = Type.LONG;
    this.defaultValue = defaultValue;
    this.temporalUnit = null;
  }

  CheckpointManagerConfigKey(String value, String defaultValue) {
    this.value = value;
    this.type = Type.STRING;
    this.defaultValue = defaultValue;
    this.temporalUnit = null;
  }

  CheckpointManagerConfigKey(String value, TemporalUnit temporalUnit) {
    this.value = value;
    this.type = Type.DURATION;
    this.defaultValue = null;
    this.temporalUnit = temporalUnit;
  }

  CheckpointManagerConfigKey(String value, Map<String, Object> defaultValue) {
    this.value = value;
    this.type = Type.MAP;
    this.defaultValue = defaultValue;
    this.temporalUnit = null;
  }

  static CheckpointManagerConfigKey toCheckpointManagerConfigKey(String value) {
    return VALUE_MAP.get(value);
  }

  /**
   * Get the key value for this enum (i.e., heron.directory.home)
   *
   * @return key value
   */
  String value() {
    return value;
  }

  Type getType() {
    return type;
  }

  TemporalUnit getTemporalUnit() {
    return temporalUnit;
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
