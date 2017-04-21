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

package com.twitter.heron.common.config;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.twitter.heron.common.basics.TypeUtils;

/**
 * SystemConfig are a set of configuration parameters that are set by the system
 * All the config associated with time is in the unit of milli-seconds, unless otherwise specified.
 * All the config associated with data is in the unit of bytes, unless otherwise specified.
 */
public final class SystemConfig {
  /**
   * Bean name for SingletonRegistry
   */
  public static final String HERON_SYSTEM_CONFIG = SystemConfig.class.getName();

  private Map<String, Object> config = new HashMap<>();

  private SystemConfig(Builder build) {
    this.config = new HashMap<>(build.keyValues);
  }

  public static Builder newBuilder(boolean loadDefaults) {
    return Builder.create(loadDefaults);
  }

  public int getInstanceInternalMetricsWriteQueueCapacity() {
    return getInteger(SystemConfigKey.INSTANCE_INTERNAL_METRICS_WRITE_QUEUE_CAPACITY);
  }

  public int getInstanceTuningExpectedMetricsWriteQueueSize() {
    return getInteger(SystemConfigKey.INSTANCE_TUNING_EXPECTED_METRICS_WRITE_QUEUE_SIZE);
  }

  public int getInstanceSetDataTupleCapacity() {
    return getInteger(SystemConfigKey.INSTANCE_SET_DATA_TUPLE_CAPACITY);
  }

  public long getInstanceSetDataTupleSizeBytes() {
    return getLong(SystemConfigKey.INSTANCE_SET_DATA_TUPLE_SIZE_BYTES);
  }

  public int getInstanceSetControlTupleCapacity() {
    return getInteger(SystemConfigKey.INSTANCE_SET_CONTROL_TUPLE_CAPACITY);
  }

  public long getInstanceForceExitTimeoutMs() {
    return getLong(SystemConfigKey.INSTANCE_FORCE_EXIT_TIMEOUT_MS);
  }

  public int getInstanceStateCheckIntervalSec() {
    return getInteger(SystemConfigKey.INSTANCE_STATE_CHECK_INTERVAL_SEC);
  }

  public int getInstanceInternalBoltReadQueueCapacity() {
    return getInteger(SystemConfigKey.INSTANCE_INTERNAL_BOLT_READ_QUEUE_CAPACITY);
  }

  public int getInstanceInternalBoltWriteQueueCapacity() {
    return getInteger(SystemConfigKey.INSTANCE_INTERNAL_BOLT_WRITE_QUEUE_CAPACITY);
  }

  public int getInstanceInternalSpoutReadQueueCapacity() {
    return getInteger(SystemConfigKey.INSTANCE_INTERNAL_SPOUT_READ_QUEUE_CAPACITY);
  }

  public int getInstanceInternalSpoutWriteQueueCapacity() {
    return getInteger(SystemConfigKey.INSTANCE_INTERNAL_SPOUT_WRITE_QUEUE_CAPACITY);
  }

  public long getInstanceAckBatchTimeMs() {
    return getLong(SystemConfigKey.INSTANCE_ACK_BATCH_TIME_MS);
  }

  public int getInstanceTuningExpectedBoltReadQueueSize() {
    return getInteger(SystemConfigKey.INSTANCE_TUNING_EXPECTED_BOLT_READ_QUEUE_SIZE);
  }

  public int getInstanceTuningExpectedBoltWriteQueueSize() {
    return getInteger(SystemConfigKey.INSTANCE_TUNING_EXPECTED_BOLT_WRITE_QUEUE_SIZE);
  }

  public int getInstanceTuningExpectedSpoutReadQueueSize() {
    return getInteger(SystemConfigKey.INSTANCE_TUNING_EXPECTED_SPOUT_READ_QUEUE_SIZE);
  }

  public int getInstanceTuningExpectedSpoutWriteQueueSize() {
    return getInteger(SystemConfigKey.INSTANCE_TUNING_EXPECTED_SPOUT_WRITE_QUEUE_SIZE);
  }

  public String getHeronLoggingDirectory() {
    return getString(SystemConfigKey.HERON_LOGGING_DIRECTORY);
  }

  public int getHeronLoggingMaximumSizeMb() {
    return getInteger(SystemConfigKey.HERON_LOGGING_MAXIMUM_SIZE_MB);
  }

  public int getHeronLoggingMaximumFiles() {
    return getInteger(SystemConfigKey.HERON_LOGGING_MAXIMUM_FILES);
  }

  public int getHeronMetricsExportIntervalSec() {
    return getInteger(SystemConfigKey.HERON_METRICS_EXPORT_INTERVAL_SEC);
  }

  public long getInstanceNetworkReadBatchTimeMs() {
    return getLong(SystemConfigKey.INSTANCE_NETWORK_READ_BATCH_TIME_MS);
  }

  public long getInstanceNetworkReadBatchSizeBytes() {
    return getLong(SystemConfigKey.INSTANCE_NETWORK_READ_BATCH_SIZE_BYTES);
  }

  public long getInstanceNetworkWriteBatchTimeMs() {
    return getLong(SystemConfigKey.INSTANCE_NETWORK_WRITE_BATCH_TIME_MS);
  }

  public long getInstanceNetworkWriteBatchSizeBytes() {
    return getLong(SystemConfigKey.INSTANCE_NETWORK_WRITE_BATCH_SIZE_BYTES);
  }

  public int getInstanceNetworkOptionsSocketReceivedBufferSizeBytes() {
    return getInteger(SystemConfigKey.INSTANCE_NETWORK_OPTIONS_SOCKET_RECEIVED_BUFFER_SIZE_BYTES);
  }

  public int getInstanceNetworkOptionsSocketSendBufferSizeBytes() {
    return getInteger(SystemConfigKey.INSTANCE_NETWORK_OPTIONS_SOCKET_SEND_BUFFER_SIZE_BYTES);
  }

  public long getInstanceEmitBatchTimeMs() {
    return getLong(SystemConfigKey.INSTANCE_EMIT_BATCH_TIME_MS);
  }

  public long getInstanceEmitBatchSizeBytes() {
    return getLong(SystemConfigKey.INSTANCE_EMIT_BATCH_SIZE_BYTES);
  }

  public long getInstanceExecuteBatchTimeMs() {
    return getLong(SystemConfigKey.INSTANCE_EXECUTE_BATCH_TIME_MS);
  }

  public long getInstanceExecuteBatchSizeBytes() {
    return getLong(SystemConfigKey.INSTANCE_EXECUTE_BATCH_SIZE_BYTES);
  }

  public int getInstanceReconnectStreammgrIntervalSec() {
    return getInteger(SystemConfigKey.INSTANCE_RECONNECT_STREAMMGR_INTERVAL_SEC);
  }

  public int getInstanceReconnectMetricsmgrIntervalSec() {
    return getInteger(SystemConfigKey.INSTANCE_RECONNECT_METRICSMGR_INTERVAL_SEC);
  }

  public int getInstanceMetricsSystemSampleIntervalSec() {
    return getInteger(SystemConfigKey.INSTANCE_METRICS_SYSTEM_SAMPLE_INTERVAL_SEC);
  }

  public int getInstanceAcknowledgementNbuckets() {
    return getInteger(SystemConfigKey.INSTANCE_ACKNOWLEDGEMENT_NBUCKETS);
  }

  public int getInstanceSlaveFetchPplanIntervalSec() {
    return getInteger(SystemConfigKey.INSTANCE_SLAVE_FETCH_PPLAN_INTERVAL_SEC);
  }

  public long getInstanceTuningIntervalMs() {
    return getLong(SystemConfigKey.INSTANCE_TUNING_INTERVAL_MS);
  }

  public double getInstanceTuningCurrentSampleWeight() {
    return getDouble(SystemConfigKey.INSTANCE_TUNING_CURRENT_SAMPLE_WEIGHT);
  }

  public long getMetricsMgrNetworkReadBatchTimeMs() {
    return getLong(SystemConfigKey.METRICSMGR_NETWORK_READ_BATCH_TIME_MS);
  }

  public long getMetricsMgrNetworkReadBatchSizeBytes() {
    return getLong(SystemConfigKey.METRICSMGR_NETWORK_READ_BATCH_SIZE_BYTES);
  }

  public long getMetricsMgrNetworkWriteBatchTimeMs() {
    return getLong(SystemConfigKey.METRICSMGR_NETWORK_WRITE_BATCH_TIME_MS);
  }

  public long getMetricsMgrNetworkWriteBatchSizeBytes() {
    return getLong(SystemConfigKey.METRICSMGR_NETWORK_WRITE_BATCH_SIZE_BYTES);
  }

  public int getMetricsMgrNetworkOptionsSocketReceivedBufferSizeBytes() {
    return getInteger(SystemConfigKey.METRICSMGR_NETWORK_OPTIONS_SOCKET_RECEIVED_BUFFER_SIZE_BYTES);
  }

  public int getMetricsMgrNetworkOptionsSocketSendBufferSizeBytes() {
    return getInteger(SystemConfigKey.METRICSMGR_NETWORK_OPTIONS_SOCKET_SEND_BUFFER_SIZE_BYTES);
  }

  public int getHeronMetricsMaxExceptionsPerMessageCount() {
    return getInteger(SystemConfigKey.HERON_METRICS_MAX_EXCEPTIONS_PER_MESSAGE_COUNT);
  }

  public long getTmasterMetricsCollectorMaximumException() {
    try {
      return getLong(SystemConfigKey.TMASTER_METRICS_COLLECTOR_MAXIMUM_EXCEPTION);
    } catch (IllegalArgumentException e) {
      return 256; // default value if not found in config
    }
  }

  public long getTmasterMetricsCollectorMaximumIntervalMin() {
    try {
      return getLong(SystemConfigKey.TMASTER_METRICS_COLLECTOR_MAXIMUM_INTERVAL_MIN);
    } catch (IllegalArgumentException e) {
      return 180; // default value if not found in config
    }
  }

  public long getTmasterMetricsCollectorPurgeIntervalSec() {
    try {
      return getLong(SystemConfigKey.TMASTER_METRICS_COLLECTOR_PURGE_INTERVAL_SEC);
    } catch (IllegalArgumentException e) {
      return 60; // default value if not found in config
    }
  }

  private String getString(SystemConfigKey key) {
    assertType(key, SystemConfigKey.Type.STRING);
    return (String) get(key);
  }

  private Integer getInteger(SystemConfigKey key) {
    assertType(key, SystemConfigKey.Type.INTEGER);
    return TypeUtils.getInteger(get(key));
  }

  private Long getLong(SystemConfigKey key) {
    assertType(key, SystemConfigKey.Type.LONG);
    return TypeUtils.getLong(get(key));
  }

  private Double getDouble(SystemConfigKey key) {
    assertType(key, SystemConfigKey.Type.DOUBLE);
    return TypeUtils.getDouble(get(key));
  }

  private Object get(SystemConfigKey key) {
    return config.get(key.value());
  }

  private void assertType(SystemConfigKey key, SystemConfigKey.Type type) {
    if (key.getType() != type) {
      throw new IllegalArgumentException(String.format(
          "config key %s is of type %s instead of expected type %s", key, key.getType(), type));
    }
  }

  public static class Builder {
    private final Map<String, Object> keyValues = new HashMap<>();

    private static SystemConfig.Builder create(boolean loadDefaults) {
      SystemConfig.Builder cb = new Builder();

      if (loadDefaults) {
        loadDefaults(cb, SystemConfigKey.values());
      }

      return cb;
    }

    private static void loadDefaults(SystemConfig.Builder cb, SystemConfigKey... keys) {
      for (SystemConfigKey key : keys) {
        if (key.getDefault() != null) {
          cb.put(key, key.getDefault());
        }
      }
    }

    public Builder put(SystemConfigKey key, Object value) {
      convertAndAdd(this.keyValues, key, value);
      return this;
    }

    public Builder putAll(String fileName, boolean mustExist) {
      File file = new File(fileName);
      if (!file.exists() && mustExist) {
        throw new IllegalArgumentException(
            String.format("Config file %s does not exist", fileName));
      }

      // convert to the correct type upon load eagerly verify type correctness
      Map<String, Object> configValues = ConfigReader.loadFile(fileName);
      for (String keyValue : configValues.keySet()) {
        SystemConfigKey key = SystemConfigKey.toSystemConfigKey(keyValue);
        if (key != null) { // sometimes config have non-java configs without an enum SystemConfigKey
          convertAndAdd(configValues, key, configValues.get(keyValue));
        }
      }
      keyValues.putAll(configValues);
      return this;
    }

    private static void convertAndAdd(Map<String, Object> config,
                                      SystemConfigKey key, Object value) {
      if (key != null) { // sometimes config have non-java configs without an enum SystemConfigKey
        switch(key.getType()) {
          case BOOLEAN:
            config.put(key.value(), TypeUtils.getBoolean(value));
            break;
          case BYTE_AMOUNT:
            config.put(key.value(), TypeUtils.getByteAmount(value));
            break;
          case DOUBLE:
            config.put(key.value(), TypeUtils.getDouble(value));
            break;
          case INTEGER:
            config.put(key.value(), TypeUtils.getInteger(value));
            break;
          case LONG:
            config.put(key.value(), TypeUtils.getLong(value));
            break;
          case STRING:
            break;
          default:
            throw new IllegalArgumentException(String.format(
                "config key %s is of type %s which is not yet supported", key, key.getType()));
        }
      }
    }

    public SystemConfig build() {
      return new SystemConfig(this);
    }
  }
}
