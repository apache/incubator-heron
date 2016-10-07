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
public class SystemConfig {
  /**
   * Bean name for SingletonRegistry
   */
  public static final String HERON_SYSTEM_CONFIG = SystemConfig.class.getName();

  /**
   * The relative path to the logging directory
   */
  public static final String HERON_LOGGING_DIRECTORY = "heron.logging.directory";

  /**
   * The maximum log file size in MB
   */
  public static final String HERON_LOGGING_MAXIMUM_SIZE_MB = "heron.logging.maximum.size.mb";

  /**
   * The maximum number of log files
   */
  public static final String HERON_LOGGING_MAXIMUM_FILES = "heron.logging.maximum.files";

  /**
   * The threshold level to log error
   */
  public static final String HERON_LOGGING_ERR_THRESHOLD = "heron.logging.err.threshold";

  /**
   * The interval in seconds to get and reset the system metrics.
   * The metrics collected will be sent to metrics manager
   */
  public static final String HERON_METRICS_EXPORT_INTERVAL_SEC
      = "heron.metrics.export.interval.sec";

  /**
   * The maximum number of exceptions in one MetricPublisherPublishMessage protobuf
   */
  public static final String HERON_METRICS_MAX_EXCEPTIONS_PER_MESSAGE_COUNT
      = "heron.metrics.max.exceptions.per.message.count";

  /**
   * The queue capacity (num of items) in bolt for buffer packets to read from stream manager
   */
  public static final String INSTANCE_INTERNAL_BOLT_READ_QUEUE_CAPACITY
      = "heron.instance.internal.bolt.read.queue.capacity";

  /**
   * The queue capacity (num of items) in bolt for buffer packets to write to stream manager
   */
  public static final String INSTANCE_INTERNAL_BOLT_WRITE_QUEUE_CAPACITY
      = "heron.instance.internal.bolt.write.queue.capacity";

  /**
   * The queue capacity (num of items) in spout for buffer packets to read from stream manager
   */
  public static final String INSTANCE_INTERNAL_SPOUT_READ_QUEUE_CAPACITY
      = "heron.instance.internal.spout.read.queue.capacity";

  /**
   * The queue capacity (num of items) in spout for buffer packets to write to stream manager
   */
  public static final String INSTANCE_INTERNAL_SPOUT_WRITE_QUEUE_CAPACITY
      = "heron.instance.internal.spout.write.queue.capacity";

  /**
   * The queue capacity (num of items) for metrics packets to write to metrics manager
   */
  public static final String INSTANCE_INTERNAL_METRICS_WRITE_QUEUE_CAPACITY
      = "heron.instance.internal.metrics.write.queue.capacity";

  /**
   * Time based, the maximum batch time in ms for instance to read from stream manager per attempt
   */
  public static final String INSTANCE_NETWORK_READ_BATCH_TIME_MS
      = "heron.instance.network.read.batch.time.ms";

  /**
   * Size based,the maximum batch size in bytes to read from stream manager
   */
  public static final String INSTANCE_NETWORK_READ_BATCH_SIZE_BYTES
      = "heron.instance.network.read.batch.size.bytes";

  /**
   * Time based, the maximum batch time in ms for instance to read from stream manager per attempt
   */
  public static final String INSTANCE_NETWORK_WRITE_BATCH_TIME_MS
      = "heron.instance.network.write.batch.time.ms";

  /**
   * Size based, the maximum batch size in bytes to write to stream manager
   */
  public static final String INSTANCE_NETWORK_WRITE_BATCH_SIZE_BYTES
      = "heron.instance.network.write.batch.size.bytes";

  /**
   * # The maximum socket's received buffer size in bytes of instance's network options
   */
  public static final String INSTANCE_NETWORK_OPTIONS_SOCKET_RECEIVED_BUFFER_SIZE_BYTES
      = "heron.instance.network.options.socket.received.buffer.size.bytes";

  /**
   * The maximum socket's send buffer size in bytes
   */
  public static final String INSTANCE_NETWORK_OPTIONS_SOCKET_SEND_BUFFER_SIZE_BYTES
      = "heron.instance.network.options.socket.send.buffer.size.bytes";

  /**
   * The maximum # of data tuple to batch in a HeronDataTupleSet protobuf
   */
  public static final String INSTANCE_SET_DATA_TUPLE_CAPACITY
      = "heron.instance.set.data.tuple.capacity";

  /**
   * The maximum size in bytes of data tuple to batch in a HeronDataTupleSet protobuf
   */
  public static final String INSTANCE_SET_DATA_TUPLE_SIZE_BYTES
      = "heron.instance.set.data.tuple.size.bytes";

  /**
   * The size of packets read from stream manager will be determined by the minimal of
   * (a) time based (b) size based
   */

  /**
   * The maximum # of control tuple to batch in a HeronControlTupleSet protobuf
   */
  public static final String INSTANCE_SET_CONTROL_TUPLE_CAPACITY
      = "heron.instance.set.control.tuple.capacity";

  /**
   * The maximum time in ms for an spout to do acknowledgement per attempt
   */
  public static final String INSTANCE_ACK_BATCH_TIME_MS = "heron.instance.ack.batch.time.ms";

  /**
   * The size of packets written to stream manager will be determined by the minimal of
   * (a) time based (b) size based
   */

  /**
   * The maximum time in ms for an spout instance to emit tuples per attempt
   */
  public static final String INSTANCE_EMIT_BATCH_TIME_MS = "heron.instance.emit.batch.time.ms";

  /**
   * The maximum batch size in bytes for an spout instance to emit tuples per attempt
   */
  public static final String INSTANCE_EMIT_BATCH_SIZE_BYTES
      = "heron.instance.emit.batch.size.bytes";

  /**
   * The maximum time in ms for an bolt instance to execute tuples per attempt
   */
  public static final String INSTANCE_EXECUTE_BATCH_TIME_MS
      = "heron.instance.execute.batch.time.ms";

  /**
   * The maximum batch size in bytes for an bolt instance to execute tuples per attempt
   */
  public static final String INSTANCE_EXECUTE_BATCH_SIZE_BYTES
      = "heron.instance.execute.batch.size.bytes";

  /**
   * The time interval for an instance to check the state change, for instance,
   * the interval a spout using to check whether activate/deactivate is invoked
   */
  public static final String INSTANCE_STATE_CHECK_INTERVAL_SEC
      = "heron.instance.state.check.interval.sec";

  /**
   * The time to wait before the instance exits forcibly when uncaught exception happens
   */
  public static final String INSTANCE_FORCE_EXIT_TIMEOUT_MS
      = "heron.instance.force.exit.timeout.ms";

  /**
   * Interval in seconds to reconnect to the stream manager
   */
  public static final String INSTANCE_RECONNECT_STREAMMGR_INTERVAL_SEC
      = "heron.instance.reconnect.streammgr.interval.sec";

  /**
   * Interval in seconds to reconnect to the metrics manager
   */
  public static final String INSTANCE_RECONNECT_METRICSMGR_INTERVAL_SEC
      = "heron.instance.reconnect.metricsmgr.interval.sec";

  /**
   * The interval in seconds to sample a system metric, for instance, jvm used memory.
   */
  public static final String INSTANCE_METRICS_SYSTEM_SAMPLE_INTERVAL_SEC
      = "heron.instance.metrics.system.sample.interval.sec";

  /**
   * The lookForTimeout Interval in spout instance will be timeoutInSeconds / NBUCKETS
   * For instance, if a tuple's timeout is 30s, and NBUCKETS is 10
   * The spout instance will check whether there are timeout tuples every 3 seconds
   */
  public static final String INSTANCE_ACKNOWLEDGEMENT_NBUCKETS
      = "heron.instance.acknowledgement.nbuckets";

  /**
   * The interval for different threads to attempt to fetch physical plan from SingletonRegistry
   */
  public static final String INSTANCE_SLAVE_FETCH_PPLAN_INTERVAL_SEC
      = "heron.instance.slave.fetch.pplan.interval.sec";

  /**
   * The expected size on read queue in bolt
   */
  public static final String INSTANCE_TUNING_EXPECTED_BOLT_READ_QUEUE_SIZE
      = "heron.instance.tuning.expected.bolt.read.queue.size";

  /**
   * The expected size on write queue in bolt
   */
  public static final String INSTANCE_TUNING_EXPECTED_BOLT_WRITE_QUEUE_SIZE
      = "heron.instance.tuning.expected.bolt.write.queue.size";

  /**
   * The expected size on read queue in spout
   */
  public static final String INSTANCE_TUNING_EXPECTED_SPOUT_READ_QUEUE_SIZE
      = "heron.instance.tuning.expected.spout.read.queue.size";

  /**
   * The exepected size on write queue in spout
   */
  public static final String INSTANCE_TUNING_EXPECTED_SPOUT_WRITE_QUEUE_SIZE
      = "heron.instance.tuning.expected.spout.write.queue.size";

  /**
   * The expected size on metrics write queue
   */
  public static final String INSTANCE_TUNING_EXPECTED_METRICS_WRITE_QUEUE_SIZE
      = "heron.instance.tuning.expected.metrics.write.queue.size";

  /**
   * During dynamically tuning, the weight of new sample size to calculate
   * the available capacity of queue.
   * We use Exponential moving average: En = (1-w) * En-1 + w * An
   * http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average
   */
  public static final String INSTANCE_TUNING_CURRENT_SAMPLE_WEIGHT
      = "heron.instance.tuning.current.sample.weight";

  /**
   * Interval in ms to tune the size of in &amp; out data queue in instance
   */
  public static final String INSTANCE_TUNING_INTERVAL_MS = "heron.instance.tuning.interval.ms";

  /**
   * The size of packets read from socket will be determined by the minimal of:
   * (a) time based (b) size based
   * <p>
   * Time based, the maximum batch time in ms for instance to read from socket per attempt
   */
  public static final String METRICSMGR_NETWORK_READ_BATCH_TIME_MS
      = "heron.metricsmgr.network.read.batch.time.ms";

  /**
   * Size based,the maximum batch size in bytes to read from socket
   */
  public static final String METRICSMGR_NETWORK_READ_BATCH_SIZE_BYTES
      = "heron.metricsmgr.network.read.batch.size.bytes";

  /**
   * The size of packets written to socket will be determined by the minimal of
   * (a) time based (b) size based
   * <p>
   * Time based, the maximum batch time in ms to write to socket
   */
  public static final String METRICSMGR_NETWORK_WRITE_BATCH_TIME_MS
      = "heron.metricsmgr.network.write.batch.time.ms";

  /**
   * Size based, the maximum batch size in bytes to write to socket
   */
  public static final String METRICSMGR_NETWORK_WRITE_BATCH_SIZE_BYTES
      = "heron.metricsmgr.network.write.batch.size.bytes";

  /**
   * The maximum socket's received buffer size in bytes
   */
  public static final String METRICSMGR_NETWORK_OPTIONS_SOCKET_RECEIVED_BUFFER_SIZE_BYTES
      = "heron.metricsmgr.network.options.socket.received.buffer.size.bytes";

  /**
   * The maximum socket's send buffer size in bytes
   */
  public static final String METRICSMGR_NETWORK_OPTIONS_SOCKET_SEND_BUFFER_SIZE_BYTES
      = "heron.metricsmgr.network.options.socket.send.buffer.size.bytes";

  private Map<String, Object> config = new HashMap<>();

  public SystemConfig() {
  }

  public SystemConfig(String configFile) {
    this(configFile, true);
  }

  public SystemConfig(String configFile, boolean mustExist) {
    super();
    this.config = findAndReadLocalFile(configFile, mustExist);
  }

  public static Map<String, Object> findAndReadLocalFile(String name, boolean mustExist) {
    File file = new File(name);
    if (!file.exists() && mustExist) {
      throw new RuntimeException(String.format("Config file %s does not exist", name));
    }

    return ConfigReader.loadFile(name);
  }

  public int getInstanceInternalMetricsWriteQueueCapacity() {
    return TypeUtils.getInteger(this.config.get(
        SystemConfig.INSTANCE_INTERNAL_METRICS_WRITE_QUEUE_CAPACITY));
  }

  public int getInstanceTuningExpectedMetricsWriteQueueSize() {
    return TypeUtils.getInteger(this.config.get(
        SystemConfig.INSTANCE_TUNING_EXPECTED_METRICS_WRITE_QUEUE_SIZE));
  }

  public int getInstanceSetDataTupleCapacity() {
    return TypeUtils.getInteger(this.config.get(
        SystemConfig.INSTANCE_SET_DATA_TUPLE_CAPACITY));
  }

  public long getInstanceSetDataTupleSizeBytes() {
    // TODO(mfu): Provide default values for all configs uniformly
    // TODO(mfu): https://github.com/twitter/heron/issues/970
    Object value = this.config.get(
        SystemConfig.INSTANCE_SET_DATA_TUPLE_SIZE_BYTES);
    return value == null ? Long.MAX_VALUE : TypeUtils.getLong(value);
  }

  public int getInstanceSetControlTupleCapacity() {
    return TypeUtils.getInteger(this.config.get(
        SystemConfig.INSTANCE_SET_CONTROL_TUPLE_CAPACITY));
  }

  public long getInstanceForceExitTimeoutMs() {
    return TypeUtils.getLong(this.config.get(
        SystemConfig.INSTANCE_FORCE_EXIT_TIMEOUT_MS));
  }

  public int getInstanceStateCheckIntervalSec() {
    return TypeUtils.getInteger(this.config.get(
        SystemConfig.INSTANCE_STATE_CHECK_INTERVAL_SEC));
  }

  public int getInstanceInternalBoltReadQueueCapacity() {
    return TypeUtils.getInteger(this.config.get(
        SystemConfig.INSTANCE_INTERNAL_BOLT_READ_QUEUE_CAPACITY));
  }

  public int getInstanceInternalBoltWriteQueueCapacity() {
    return TypeUtils.getInteger(this.config.get(
        SystemConfig.INSTANCE_INTERNAL_BOLT_WRITE_QUEUE_CAPACITY));
  }

  public int getInstanceInternalSpoutReadQueueCapacity() {
    return TypeUtils.getInteger(this.config.get(
        SystemConfig.INSTANCE_INTERNAL_SPOUT_READ_QUEUE_CAPACITY));
  }

  public int getInstanceInternalSpoutWriteQueueCapacity() {
    return TypeUtils.getInteger(this.config.get(
        SystemConfig.INSTANCE_INTERNAL_SPOUT_WRITE_QUEUE_CAPACITY));
  }

  public long getInstanceAckBatchTimeMs() {
    return TypeUtils.getLong(this.config.get(SystemConfig.INSTANCE_ACK_BATCH_TIME_MS));
  }

  public int getInstanceTuningExpectedBoltReadQueueSize() {
    return TypeUtils.getInteger(this.config.get(
        SystemConfig.INSTANCE_TUNING_EXPECTED_BOLT_READ_QUEUE_SIZE));
  }

  public int getInstanceTuningExpectedBoltWriteQueueSize() {
    return TypeUtils.getInteger(this.config.get(
        SystemConfig.INSTANCE_TUNING_EXPECTED_BOLT_WRITE_QUEUE_SIZE));
  }

  public int getInstanceTuningExpectedSpoutReadQueueSize() {
    return TypeUtils.getInteger(this.config.get(
        SystemConfig.INSTANCE_TUNING_EXPECTED_SPOUT_READ_QUEUE_SIZE));
  }

  public int getInstanceTuningExpectedSpoutWriteQueueSize() {
    return TypeUtils.getInteger(this.config.get(
        SystemConfig.INSTANCE_TUNING_EXPECTED_SPOUT_WRITE_QUEUE_SIZE));
  }

  public String getHeronLoggingDirectory() {
    return (String) this.config.get(SystemConfig.HERON_LOGGING_DIRECTORY);
  }

  public int getHeronLoggingMaximumSizeMb() {
    return TypeUtils.getInteger(this.config.get(SystemConfig.HERON_LOGGING_MAXIMUM_SIZE_MB));
  }

  public int getHeronLoggingMaximumFiles() {
    return TypeUtils.getInteger(this.config.get(SystemConfig.HERON_LOGGING_MAXIMUM_FILES));
  }

  public int getHeronMetricsExportIntervalSec() {
    return TypeUtils.getInteger(this.config.get(SystemConfig.HERON_METRICS_EXPORT_INTERVAL_SEC));
  }

  public long getInstanceNetworkReadBatchTimeMs() {
    return TypeUtils.getLong(this.config.get(SystemConfig.INSTANCE_NETWORK_READ_BATCH_TIME_MS));
  }

  public long getInstanceNetworkReadBatchSizeBytes() {
    return TypeUtils.getLong(this.config.get(SystemConfig.INSTANCE_NETWORK_READ_BATCH_SIZE_BYTES));
  }

  public long getInstanceNetworkWriteBatchTimeMs() {
    return TypeUtils.getLong(this.config.get(SystemConfig.INSTANCE_NETWORK_WRITE_BATCH_TIME_MS));
  }

  public long getInstanceNetworkWriteBatchSizeBytes() {
    return TypeUtils.getLong(this.config.get(SystemConfig.INSTANCE_NETWORK_WRITE_BATCH_SIZE_BYTES));
  }

  public int getInstanceNetworkOptionsSocketReceivedBufferSizeBytes() {
    return TypeUtils.
        getInteger(this.config.get(
            SystemConfig.INSTANCE_NETWORK_OPTIONS_SOCKET_RECEIVED_BUFFER_SIZE_BYTES));
  }

  public int getInstanceNetworkOptionsSocketSendBufferSizeBytes() {
    return TypeUtils.
        getInteger(this.config.get(
            SystemConfig.INSTANCE_NETWORK_OPTIONS_SOCKET_SEND_BUFFER_SIZE_BYTES));
  }

  public long getInstanceEmitBatchTimeMs() {
    return TypeUtils.getLong(this.config.get(SystemConfig.INSTANCE_EMIT_BATCH_TIME_MS));
  }

  public long getInstanceEmitBatchSizeBytes() {
    return TypeUtils.getLong(this.config.get(SystemConfig.INSTANCE_EMIT_BATCH_SIZE_BYTES));
  }

  public long getInstanceExecuteBatchTimeMs() {
    return TypeUtils.getLong(this.config.get(SystemConfig.INSTANCE_EXECUTE_BATCH_TIME_MS));
  }

  public long getInstanceExecuteBatchSizeBytes() {
    return TypeUtils.getLong(this.config.get(SystemConfig.INSTANCE_EXECUTE_BATCH_SIZE_BYTES));
  }

  public int getInstanceReconnectStreammgrIntervalSec() {
    return TypeUtils.
        getInteger(this.config.get(SystemConfig.INSTANCE_RECONNECT_STREAMMGR_INTERVAL_SEC));
  }

  public int getInstanceReconnectMetricsmgrIntervalSec() {
    return TypeUtils.
        getInteger(this.config.get(SystemConfig.INSTANCE_RECONNECT_METRICSMGR_INTERVAL_SEC));
  }

  public int getInstanceMetricsSystemSampleIntervalSec() {
    return TypeUtils.getInteger(this.config.get(
        SystemConfig.INSTANCE_METRICS_SYSTEM_SAMPLE_INTERVAL_SEC));
  }

  public int getInstanceAcknowledgementNbuckets() {
    return TypeUtils.getInteger(this.config.get(SystemConfig.INSTANCE_ACKNOWLEDGEMENT_NBUCKETS));
  }

  public int getInstanceSlaveFetchPplanIntervalSec() {
    return TypeUtils.getInteger(
        this.config.get(SystemConfig.INSTANCE_SLAVE_FETCH_PPLAN_INTERVAL_SEC));
  }

  public long getInstanceTuningIntervalMs() {
    return TypeUtils.getLong(this.config.get(SystemConfig.INSTANCE_TUNING_INTERVAL_MS));
  }

  public double getInstanceTuningCurrentSampleWeight() {
    return Double.parseDouble(this.config.get(
        SystemConfig.INSTANCE_TUNING_CURRENT_SAMPLE_WEIGHT).toString());
  }

  public long getMetricsMgrNetworkReadBatchTimeMs() {
    return TypeUtils.getLong(
        this.config.get(SystemConfig.METRICSMGR_NETWORK_READ_BATCH_TIME_MS));
  }

  public long getMetricsMgrNetworkReadBatchSizeBytes() {
    return TypeUtils.getLong(this.config.get(
        SystemConfig.METRICSMGR_NETWORK_READ_BATCH_SIZE_BYTES));
  }

  public long getMetricsMgrNetworkWriteBatchTimeMs() {
    return TypeUtils.getLong(this.config.get(SystemConfig.METRICSMGR_NETWORK_WRITE_BATCH_TIME_MS));
  }

  public long getMetricsMgrNetworkWriteBatchSizeBytes() {
    return TypeUtils.getLong(this.config.get(
        SystemConfig.METRICSMGR_NETWORK_WRITE_BATCH_SIZE_BYTES));
  }

  public int getMetricsMgrNetworkOptionsSocketReceivedBufferSizeBytes() {
    return TypeUtils.
        getInteger(this.config.get(
            SystemConfig.METRICSMGR_NETWORK_OPTIONS_SOCKET_RECEIVED_BUFFER_SIZE_BYTES));
  }

  public int getMetricsMgrNetworkOptionsSocketSendBufferSizeBytes() {
    return TypeUtils.
        getInteger(this.config.get(
            SystemConfig.METRICSMGR_NETWORK_OPTIONS_SOCKET_SEND_BUFFER_SIZE_BYTES));
  }

  public int getHeronMetricsMaxExceptionsPerMessageCount() {
    Object value = this.config.get(
        SystemConfig.HERON_METRICS_MAX_EXCEPTIONS_PER_MESSAGE_COUNT);
    return value == null ? Integer.MAX_VALUE : TypeUtils.getInteger(value);
  }

  public Object put(String key, Object value) {
    return this.config.put(key, value);
  }
}

