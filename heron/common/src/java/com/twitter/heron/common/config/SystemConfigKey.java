//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.common.config;

import java.util.HashMap;
import java.util.Map;

import com.twitter.heron.common.basics.ByteAmount;

/**
 * Keys to be used in the SystemConfigs
 */
// TODO(mfu): Provide default values for all configs uniformly
// TODO(mfu): https://github.com/twitter/heron/issues/970
public enum SystemConfigKey {
  /**
   * The relative path to the logging directory
   */
  HERON_LOGGING_DIRECTORY("heron.logging.directory", Type.STRING),

  /**
   * The maximum log file size in MB
   */
  HERON_LOGGING_MAXIMUM_SIZE_MB("heron.logging.maximum.size.mb", Type.INTEGER),

  /**
   * The maximum number of log files
   */
  HERON_LOGGING_MAXIMUM_FILES("heron.logging.maximum.files", Type.INTEGER),

  /**
   * The threshold level to log error
   */
  HERON_LOGGING_ERR_THRESHOLD("heron.logging.err.threshold", Type.INTEGER),

  /**
   * The interval in seconds to get and reset the system metrics.
   * The metrics collected will be sent to metrics manager
   */
  HERON_METRICS_EXPORT_INTERVAL_SEC("heron.metrics.export.interval.sec", Type.INTEGER),

  /**
   * The maximum number of exceptions in one MetricPublisherPublishMessage protobuf
   */
  HERON_METRICS_MAX_EXCEPTIONS_PER_MESSAGE_COUNT(
      "heron.metrics.max.exceptions.per.message.count", Integer.MAX_VALUE),

  /**
   * The queue capacity (num of items) in bolt for buffer packets to read from stream manager
   */
  INSTANCE_INTERNAL_BOLT_READ_QUEUE_CAPACITY(
      "heron.instance.internal.bolt.read.queue.capacity", Type.INTEGER),

  /**
   * The queue capacity (num of items) in bolt for buffer packets to write to stream manager
   */
  INSTANCE_INTERNAL_BOLT_WRITE_QUEUE_CAPACITY(
      "heron.instance.internal.bolt.write.queue.capacity", Type.INTEGER),

  /**
   * The queue capacity (num of items) in spout for buffer packets to read from stream manager
   */
  INSTANCE_INTERNAL_SPOUT_READ_QUEUE_CAPACITY(
      "heron.instance.internal.spout.read.queue.capacity", Type.INTEGER),

  /**
   * The queue capacity (num of items) in spout for buffer packets to write to stream manager
   */
  INSTANCE_INTERNAL_SPOUT_WRITE_QUEUE_CAPACITY(
      "heron.instance.internal.spout.write.queue.capacity", Type.INTEGER),

  /**
   * The queue capacity (num of items) for metrics packets to write to metrics manager
   */
  INSTANCE_INTERNAL_METRICS_WRITE_QUEUE_CAPACITY(
      "heron.instance.internal.metrics.write.queue.capacity", Type.INTEGER),

  /**
   * Time based, the maximum batch time in ms for instance to read from stream manager per attempt
   */
  INSTANCE_NETWORK_READ_BATCH_TIME_MS("heron.instance.network.read.batch.time.ms", Type.LONG),

  /**
   * Size based,the maximum batch size in bytes to read from stream manager
   */
  INSTANCE_NETWORK_READ_BATCH_SIZE(
      "heron.instance.network.read.batch.size.bytes", Type.BYTE_AMOUNT),

  /**
   * Time based, the maximum batch time in ms for instance to read from stream manager per attempt
   */
  INSTANCE_NETWORK_WRITE_BATCH_TIME_MS("heron.instance.network.write.batch.time.ms", Type.LONG),

  /**
   * Size based, the maximum batch size in bytes to write to stream manager
   */
  INSTANCE_NETWORK_WRITE_BATCH_SIZE(
      "heron.instance.network.write.batch.size.bytes", Type.BYTE_AMOUNT),

  /**
   * # The maximum socket's received buffer size in bytes of instance's network options
   */
  INSTANCE_NETWORK_OPTIONS_SOCKET_RECEIVED_BUFFER_SIZE(
      "heron.instance.network.options.socket.received.buffer.size.bytes", Type.BYTE_AMOUNT),

  /**
   * The maximum socket's send buffer size in bytes
   */
  INSTANCE_NETWORK_OPTIONS_SOCKET_SEND_BUFFER_SIZE(
      "heron.instance.network.options.socket.send.buffer.size.bytes", Type.BYTE_AMOUNT),

  /**
   * The maximum # of data tuple to batch in a HeronDataTupleSet protobuf
   */
  INSTANCE_SET_DATA_TUPLE_CAPACITY("heron.instance.set.data.tuple.capacity", Type.INTEGER),

  /**
   * The maximum size in bytes of data tuple to batch in a HeronDataTupleSet protobuf
   */
  INSTANCE_SET_DATA_TUPLE_SIZE(
      "heron.instance.set.data.tuple.size.bytes", ByteAmount.fromBytes(Long.MAX_VALUE)),

  /**
   * The size of packets read from stream manager will be determined by the minimal of
   * (a) time based (b) size based
   */

  /**
   * The maximum # of control tuple to batch in a HeronControlTupleSet protobuf
   */
  INSTANCE_SET_CONTROL_TUPLE_CAPACITY("heron.instance.set.control.tuple.capacity", Type.INTEGER),

  /**
   * The maximum time in ms for an spout to do acknowledgement per attempt
   */
  INSTANCE_ACK_BATCH_TIME_MS("heron.instance.ack.batch.time.ms", Type.LONG),

  /**
   * The size of packets written to stream manager will be determined by the minimal of
   * (a) time based (b) size based
   */

  /**
   * The maximum time in ms for an spout instance to emit tuples per attempt
   */
  INSTANCE_EMIT_BATCH_TIME_MS("heron.instance.emit.batch.time.ms", Type.LONG),

  /**
   * The maximum batch size in bytes for an spout instance to emit tuples per attempt
   */
  INSTANCE_EMIT_BATCH_SIZE("heron.instance.emit.batch.size.bytes", Type.BYTE_AMOUNT),

  /**
   * The maximum time in ms for an bolt instance to execute tuples per attempt
   */
  INSTANCE_EXECUTE_BATCH_TIME_MS("heron.instance.execute.batch.time.ms", Type.LONG),

  /**
   * The maximum batch size in bytes for an bolt instance to execute tuples per attempt
   */
  INSTANCE_EXECUTE_BATCH_SIZE("heron.instance.execute.batch.size.bytes", Type.BYTE_AMOUNT),

  /**
   * The time interval for an instance to check the state change, for instance,
   * the interval a spout using to check whether activate/deactivate is invoked
   * Slated for removal, see https://github.com/twitter/heron/issues/1712
   */
  INSTANCE_STATE_CHECK_INTERVAL_SEC("heron.instance.state.check.interval.sec", Type.INTEGER),

  /**
   * The time to wait before the instance exits forcibly when uncaught exception happens
   */
  INSTANCE_FORCE_EXIT_TIMEOUT_MS("heron.instance.force.exit.timeout.ms", Type.LONG),

  /**
   * Interval in seconds to reconnect to the stream manager
   */
  INSTANCE_RECONNECT_STREAMMGR_INTERVAL_SEC(
      "heron.instance.reconnect.streammgr.interval.sec", Type.INTEGER),

  /**
   * Interval in seconds to reconnect to the metrics manager
   */
  INSTANCE_RECONNECT_METRICSMGR_INTERVAL_SEC(
      "heron.instance.reconnect.metricsmgr.interval.sec", Type.INTEGER),

  /**
   * The interval in seconds to sample a system metric, for instance, jvm used memory.
   */
  INSTANCE_METRICS_SYSTEM_SAMPLE_INTERVAL_SEC(
      "heron.instance.metrics.system.sample.interval.sec", Type.INTEGER),

  /**
   * The lookForTimeout Interval in spout instance will be timeoutInSeconds / NBUCKETS
   * For instance, if a tuple's timeout is 30s, and NBUCKETS is 10
   * The spout instance will check whether there are timeout tuples every 3 seconds
   */
  INSTANCE_ACKNOWLEDGEMENT_NBUCKETS("heron.instance.acknowledgement.nbuckets", Type.INTEGER),

  /**
   * The interval for different threads to attempt to fetch physical plan from SingletonRegistry
   */
  INSTANCE_SLAVE_FETCH_PPLAN_INTERVAL_SEC(
      "heron.instance.slave.fetch.pplan.interval.sec", Type.INTEGER),

  /**
   * The expected size on read queue in bolt
   */
  INSTANCE_TUNING_EXPECTED_BOLT_READ_QUEUE_SIZE(
      "heron.instance.tuning.expected.bolt.read.queue.size", Type.INTEGER),

  /**
   * The expected size on write queue in bolt
   */
  INSTANCE_TUNING_EXPECTED_BOLT_WRITE_QUEUE_SIZE(
      "heron.instance.tuning.expected.bolt.write.queue.size", Type.INTEGER),

  /**
   * The expected size on read queue in spout
   */
  INSTANCE_TUNING_EXPECTED_SPOUT_READ_QUEUE_SIZE(
      "heron.instance.tuning.expected.spout.read.queue.size", Type.INTEGER),

  /**
   * The exepected size on write queue in spout
   */
  INSTANCE_TUNING_EXPECTED_SPOUT_WRITE_QUEUE_SIZE(
      "heron.instance.tuning.expected.spout.write.queue.size", Type.INTEGER),

  /**
   * The expected size on metrics write queue
   */
  INSTANCE_TUNING_EXPECTED_METRICS_WRITE_QUEUE_SIZE(
      "heron.instance.tuning.expected.metrics.write.queue.size", Type.INTEGER),

  /**
   * During dynamically tuning, the weight of new sample size to calculate
   * the available capacity of queue.
   * We use Exponential moving average: En = (1-w) * En-1 + w * An
   * http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average
   */
  INSTANCE_TUNING_CURRENT_SAMPLE_WEIGHT("heron.instance.tuning.current.sample.weight", Type.DOUBLE),

  /**
   * Interval in ms to tune the size of in &amp; out data queue in instance
   */
  INSTANCE_TUNING_INTERVAL_MS("heron.instance.tuning.interval.ms", Type.LONG),

  /**
   * The size of packets read from socket will be determined by the minimal of:
   * (a) time based (b) size based
   * <p>
   * Time based, the maximum batch time in ms for instance to read from socket per attempt
   */
  METRICSMGR_NETWORK_READ_BATCH_TIME_MS(
      "heron.metricsmgr.network.read.batch.time.ms", Type.LONG),

  /**
   * Size based,the maximum batch size in bytes to read from socket
   */
  METRICSMGR_NETWORK_READ_BATCH_SIZE(
      "heron.metricsmgr.network.read.batch.size.bytes", Type.BYTE_AMOUNT),

  /**
   * The size of packets written to socket will be determined by the minimal of
   * (a) time based (b) size based
   * <p>
   * Time based, the maximum batch time in ms to write to socket
   */
  METRICSMGR_NETWORK_WRITE_BATCH_TIME_MS(
      "heron.metricsmgr.network.write.batch.time.ms", Type.LONG),

  /**
   * Size based, the maximum batch size in bytes to write to socket
   */
  METRICSMGR_NETWORK_WRITE_BATCH_SIZE(
      "heron.metricsmgr.network.write.batch.size.bytes", Type.BYTE_AMOUNT),

  /**
   * The maximum socket's received buffer size in bytes
   */
  METRICSMGR_NETWORK_OPTIONS_SOCKET_RECEIVED_BUFFER_SIZE(
      "heron.metricsmgr.network.options.socket.received.buffer.size.bytes", Type.BYTE_AMOUNT),

  /**
   * The maximum socket's send buffer size in bytes
   */
  METRICSMGR_NETWORK_OPTIONS_SOCKET_SEND_BUFFER_SIZE(
      "heron.metricsmgr.network.options.socket.send.buffer.size.bytes", Type.BYTE_AMOUNT),

  /**
   *The maximum exception count be kept in tmaster
   */
  TMASTER_METRICS_COLLECTOR_MAXIMUM_EXCEPTION(
      "heron.tmaster.metrics.collector.maximum.exception", Type.LONG),

  /**
   * The maximum interval in minutes of metrics to be kept in tmaster
   */
  TMASTER_METRICS_COLLECTOR_MAXIMUM_INTERVAL_MIN(
      "heron.tmaster.metrics.collector.maximum.interval.min", Type.LONG),

  /**
   * The interval for tmaster to purge metrics from socket
   */
  TMASTER_METRICS_COLLECTOR_PURGE_INTERVAL_SEC(
       "heron.tmaster.metrics.collector.purge.interval.sec", Type.LONG);


  private final String value;
  private final Object defaultValue;
  private final Type type;

  public enum Type {
    BOOLEAN,
    BYTE_AMOUNT,
    DOUBLE,
    INTEGER,
    LONG,
    STRING
  }

  // Map of value -> enum
  private static final Map<String, SystemConfigKey> VALUE_MAP;

  static {
    VALUE_MAP = new HashMap<>();
    for (SystemConfigKey key : SystemConfigKey.values()) {
      VALUE_MAP.put(key.value(), key);
    }
  }

  SystemConfigKey(String value, Type type) {
    this.value = value;
    this.type = type;
    this.defaultValue = null;
  }

  SystemConfigKey(String value, String defaultValue) {
    this.value = value;
    this.type = Type.STRING;
    this.defaultValue = defaultValue;
  }

  SystemConfigKey(String value, Integer defaultValue) {
    this.value = value;
    this.type = Type.INTEGER;
    this.defaultValue = defaultValue;
  }

  SystemConfigKey(String value, Long defaultValue) {
    this.value = value;
    this.type = Type.LONG;
    this.defaultValue = defaultValue;
  }

  SystemConfigKey(String value, Double defaultValue) {
    this.value = value;
    this.type = Type.DOUBLE;
    this.defaultValue = defaultValue;
  }

  SystemConfigKey(String value, Boolean defaultValue) {
    this.value = value;
    this.type = Type.BOOLEAN;
    this.defaultValue = defaultValue;
  }

  SystemConfigKey(String value, ByteAmount defaultValue) {
    this.value = value;
    this.type = Type.BYTE_AMOUNT;
    this.defaultValue = defaultValue;
  }

  static SystemConfigKey toSystemConfigKey(String value) {
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
