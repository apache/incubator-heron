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

import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.common.basics.TypeUtils;
import org.apache.heron.common.config.ConfigReader;

public final class CheckpointManagerConfig {

  private Map<String, Object> config = new HashMap<>();

  private CheckpointManagerConfig(Builder build) {
    this.config = new HashMap<>(build.keyValues);
  }

  public static Builder newBuilder(boolean loadDefaults) {
    return Builder.create(loadDefaults);
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> getStatefulStorageConfig() {
    Object statefulStorageConfigObject = get(CheckpointManagerConfigKey.STORAGE_CONFIG);

    if (statefulStorageConfigObject instanceof Map) {
      return (Map<String, Object>) statefulStorageConfigObject;
    } else {
      throw new IllegalArgumentException(
          String.format("configs for stateful storage needs to be map, but is: %s",
              statefulStorageConfigObject.getClass().getName()));
    }
  }

  public String getStorageClassname() {
    return getString(CheckpointManagerConfigKey.STORAGE_CLASSNAME);
  }

  public ByteAmount getWriteBatchSize() {
    return getByteAmount(CheckpointManagerConfigKey.WRITE_BATCH_SIZE);
  }

  public Duration getWriteBatchTime() {
    return getDuration(CheckpointManagerConfigKey.WRITE_BATCH_TIME);
  }

  public ByteAmount getReadBatchSize() {
    return getByteAmount(CheckpointManagerConfigKey.READ_BATCH_SIZE);
  }

  public Duration getReadBatchTime() {
    return getDuration(CheckpointManagerConfigKey.READ_BATCH_TIME);
  }

  public ByteAmount getSocketSendSize() {
    return getByteAmount(CheckpointManagerConfigKey.SOCKET_SEND_SIZE);
  }

  public ByteAmount getSocketReceiveSize() {
    return getByteAmount(CheckpointManagerConfigKey.SOCKET_RECEIVE_SIZE);
  }

  public ByteAmount getMaximumPacketSize() {
    return getByteAmount(CheckpointManagerConfigKey.MAXIMUM_PACKET_SIZE);
  }

  private String getString(CheckpointManagerConfigKey key) {
    assertType(key, CheckpointManagerConfigKey.Type.STRING);
    return (String) get(key);
  }

  private Integer getInteger(CheckpointManagerConfigKey key) {
    assertType(key, CheckpointManagerConfigKey.Type.INTEGER);
    return TypeUtils.getInteger(get(key));
  }

  private Long getLong(CheckpointManagerConfigKey key) {
    assertType(key, CheckpointManagerConfigKey.Type.LONG);
    return TypeUtils.getLong(get(key));
  }

  private Duration getDuration(CheckpointManagerConfigKey key) {
    assertType(key, CheckpointManagerConfigKey.Type.DURATION);
    return TypeUtils.getDuration(get(key), key.getTemporalUnit());
  }

  private ByteAmount getByteAmount(CheckpointManagerConfigKey key) {
    assertType(key, CheckpointManagerConfigKey.Type.BYTE_AMOUNT);
    return TypeUtils.getByteAmount(get(key));
  }

  private Object get(CheckpointManagerConfigKey key) {
    return config.get(key.value());
  }

  private void assertType(CheckpointManagerConfigKey key, CheckpointManagerConfigKey.Type type) {
    if (key.getType() != type) {
      throw new IllegalArgumentException(String.format(
          "config key %s is of type %s instead of expected type %s", key, key.getType(), type));
    }
  }

  public static class Builder {
    private final Map<String, Object> keyValues = new HashMap<>();

    private static CheckpointManagerConfig.Builder create(boolean loadDefaults) {
      CheckpointManagerConfig.Builder cb = new Builder();

      if (loadDefaults) {
        loadDefaults(cb, CheckpointManagerConfigKey.values());
      }

      return cb;
    }

    private static void loadDefaults(CheckpointManagerConfig.Builder cb,
                                     CheckpointManagerConfigKey... keys) {
      for (CheckpointManagerConfigKey key : keys) {
        if (key.getDefault() != null) {
          cb.put(key, key.getDefault());
        }
      }
    }

    public Builder put(CheckpointManagerConfigKey key, Object value) {
      convertAndAdd(this.keyValues, key, value);
      return this;
    }

    public Builder putAll(String fileName, boolean mustExist) {
      File file = new File(fileName);
      if (!file.exists() && mustExist) {
        throw new IllegalArgumentException(
            String.format("Config file %s does not exist", fileName));
      }

      Map<String, Object> configValues = ConfigReader.loadFile(fileName);
      for (String keyValue : configValues.keySet()) {
        CheckpointManagerConfigKey key =
            CheckpointManagerConfigKey.toCheckpointManagerConfigKey(keyValue);
        if (key != null) {
          convertAndAdd(configValues, key, configValues.get(keyValue));
        }
      }
      keyValues.putAll(configValues);
      return this;
    }

    public Builder override(String fileName) {
      File file = new File(fileName);
      if (file.exists()) {
        Map<String, Object> overridden = ConfigReader.loadFile(fileName);
        //overridden yaml always has flattened key value pair
        keyValues.putAll(overridden);
        Object storageConfigMap = keyValues.get(CheckpointManagerConfigKey.STORAGE_CONFIG.value());
        if (storageConfigMap instanceof Map) {
          ((Map) storageConfigMap).putAll(overridden);
        }
      }
      return this;
    }


    private static void convertAndAdd(Map<String, Object> config,
                                      CheckpointManagerConfigKey key,
                                      Object value) {
      if (key != null) {
        switch (key.getType()) {
          case BYTE_AMOUNT:
            config.put(key.value(), TypeUtils.getByteAmount(value));
            break;
          case DURATION:
            config.put(key.value(), TypeUtils.getDuration(value, key.getTemporalUnit()));
            break;
          case INTEGER:
            config.put(key.value(), TypeUtils.getInteger(value));
            break;
          case LONG:
            config.put(key.value(), TypeUtils.getLong(value));
            break;
          case STRING:
            config.put(key.value(), value);
            break;
          case MAP:
            config.put(key.value(), value);
            break;
          default:
            throw new IllegalArgumentException(String.format(
                "config key %s is of type %s which is not yet supported", key, key.getType()));
        }
      }
    }

    public CheckpointManagerConfig build() {
      return new CheckpointManagerConfig(this);
    }
  }
}
