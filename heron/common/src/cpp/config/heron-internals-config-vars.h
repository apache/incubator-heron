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

////////////////////////////////////////////////////////////////
//
// topology_config_vars.h
//
// Essentially this file defines as the config variables
// heron internals system config, which should not be touched
// by users
///////////////////////////////////////////////////////////////

#ifndef HERON_INTERNALS_COFNIG_VARS_H_
#define HERON_INTERNALS_COFNIG_VARS_H_

#include <string>

#include "basics/basics.h"

namespace heron {
namespace config {

class HeronInternalsConfigVars {
 public:
  /**
  * HERON_ configs are general configurations over all componenets
  **/

  // The relative path to the logging directory
  static const sp_string HERON_LOGGING_DIRECTORY;

  // The maximum log file size in MB
  static const sp_string HERON_LOGGING_MAXIMUM_SIZE_MB;

  // The maximum number of log files
  static const sp_string HERON_LOGGING_MAXIMUM_FILES;

  // The interval in seconds after which to check if the tmanager location
  // has been fetched or not
  static const sp_string HERON_CHECK_TMANAGER_LOCATION_INTERVAL_SEC;

  // The interval in seconds to prune logging files in C+++
  static const sp_string HERON_LOGGING_PRUNE_INTERVAL_SEC;

  // The interval in seconds to flush log files in C+++
  static const sp_string HERON_LOGGING_FLUSH_INTERVAL_SEC;

  // The threadhold level to log error
  static const sp_string HERON_LOGGING_ERR_THRESHOLD;

  // The interval in seconds for different components to export metrics to metrics manager
  static const sp_string HERON_METRICS_EXPORT_INTERVAL_SEC;

  /**
  * HERON_METRICSMGR_* configs are for the metrics manager
  **/

  // The host of scribe to be exported metrics to
  static const sp_string HERON_METRICSMGR_SCRIBE_HOST;

  // The port of scribe to be exported metrics to
  static const sp_string HERON_METRICSMGR_SCRIBE_PORT;

  // The category of the scribe to be exported metrics to
  static const sp_string HERON_METRICSMGR_SCRIBE_CATEGORY;

  // The service name of the metrics in cuckoo_json
  static const sp_string HERON_METRICSMGR_SCRIBE_SERVICE_NAMESPACE;

  // The maximum retry attempts to write metrics to scribe
  static const sp_string HERON_METRICSMGR_SCRIBE_WRITE_RETRY_TIMES;

  // The timeout in seconds for metrics manager to write metrics to scribe
  static const sp_string HERON_METRICSMGR_SCRIBE_WRITE_TIMEOUT_SEC;

  // The interval in seconds to flush cached metircs to scribe
  static const sp_string HERON_METRICSMGR_SCRIBE_PERIODIC_FLUSH_INTERVAL_SEC;

  // The interval in seconds to reconnect to tmanager if a connection failure happens
  static const sp_string HERON_METRICSMGR_RECONNECT_TMANAGER_INTERVAL_SEC;

  // The maximum packet size in MB of metrics manager's network options
  static const sp_string HERON_METRICSMGR_NETWORK_OPTIONS_MAXIMUM_PACKET_MB;

  /**
  * HERON_TMANAGER_* configs are for the metrics manager
  **/

  // The maximum interval in minutes of metrics to be kept in tmanager
  static const sp_string HERON_TMANAGER_METRICS_COLLECTOR_MAXIMUM_INTERVAL_MIN;

  // The maximum time to retry to establish the tmanager
  static const sp_string HERON_TMANAGER_ESTABLISH_RETRY_TIMES;

  // The interval to retry to establish the tmanager
  static const sp_string HERON_TMANAGER_ESTABLISH_RETRY_INTERVAL_SEC;

  // The maximum packet size in MB of tmanager's network options for stmgrs to connect to
  static const sp_string HERON_TMANAGER_NETWORK_SERVER_OPTIONS_MAXIMUM_PACKET_MB;

  // The maximum packet size in MB of tmanager's network options for scheduler to connect to
  static const sp_string HERON_TMANAGER_NETWORK_CONTROLLER_OPTIONS_MAXIMUM_PACKET_MB;

  // The maximum packet size in MB of tmanager's network options for stat queries
  static const sp_string HERON_TMANAGER_NETWORK_STATS_OPTIONS_MAXIMUM_PACKET_MB;

  // The inteval for tmanager to purge metrics from socket
  static const sp_string HERON_TMANAGER_METRICS_COLLECTOR_PURGE_INTERVAL_SEC;

  // The maximum # of exception to be stored in tmetrics collector, to prevent potential OOM
  static const sp_string HERON_TMANAGER_METRICS_COLLECTOR_MAXIMUM_EXCEPTION;

  // Whether tmanager's metrics server should bind on all interfaces
  static const sp_string HERON_TMANAGER_METRICS_NETWORK_BINDALLINTERFACES;

  // The timeout in seconds for stream mgr, compared with (current time - last heartbeat time)
  static const sp_string HERON_TMANAGER_STMGR_STATE_TIMEOUT_SEC;

  /**
  * HERON_STREAMMGR_* configs are for the stream manager
  **/

  // The tuple cache (used for batching) can be drained in two ways: (a) Time based (b) size based
  // The frequency in ms to drain the tuple cache in stream manager
  static const sp_string HERON_STREAMMGR_CACHE_DRAIN_FREQUENCY_MS;

  // The sized based threshold in MB for draining the tuple cache
  static const sp_string HERON_STREAMMGR_CACHE_DRAIN_SIZE_MB;

  // The max number of messages in the memory pool for each message type
  static const sp_string HERON_STREAMMGR_MEMPOOL_MAX_MESSAGE_NUMBER;

  // For efficient acknowledgement
  static const sp_string HERON_STREAMMGR_XORMGR_ROTATINGMAP_NBUCKETS;

  // The reconnect interval to other stream managers in second for stream manager client
  static const sp_string HERON_STREAMMGR_CLIENT_RECONNECT_INTERVAL_SEC;

  // The reconnect interval to tamster in second for stream manager client
  static const sp_string HERON_STREAMMGR_CLIENT_RECONNECT_TMANAGER_INTERVAL_SEC;

  // The max reconnect attempts to tmanager for stream manager client
  static const sp_string HERON_STREAMMGR_CLIENT_RECONNECT_TMANAGER_MAX_ATTEMPTS;

  // The maximum packet size in MB of stream manager's network options
  static const sp_string HERON_STREAMMGR_NETWORK_OPTIONS_MAXIMUM_PACKET_MB;

  // The interval in seconds to send heartbeat
  static const sp_string HERON_STREAMMGR_TMANAGER_HEARTBEAT_INTERVAL_SEC;

  // Maximum batch size in MB to read by stream manager from socket
  static const sp_string HERON_STREAMMGR_CONNECTION_READ_BATCH_SIZE_MB;

  // Maximum batch size in MB to write by stream manager to socket
  static const sp_string HERON_STREAMMGR_CONNECTION_WRITE_BATCH_SIZE_MB;

  // Number of times we should wait to see a buffer full while enqueueing data before declaring
  // start of back pressure
  static const sp_string HERON_STREAMMGR_NETWORK_BACKPRESSURE_THRESHOLD;

  // High water mark on the num in MB that can be left outstanding on a connection
  static const sp_string HERON_STREAMMGR_NETWORK_BACKPRESSURE_HIGHWATERMARK_MB;

  // Low water mark on the num in MB that can be left outstanding on a connection
  static const sp_string HERON_STREAMMGR_NETWORK_BACKPRESSURE_LOWWATERMARK_MB;

  // The size based threshold in MB for buffering data tuples waiting for
  // checkpoint markers to arrive before giving up
  static const sp_string HERON_STREAMMGR_STATEFUL_BUFFER_SIZE_MB;

  /**
  * HERON_INSTANCE_* configs are for the instance
  **/

  // Interval in seconds to reconnect to the stream manager
  static const sp_string HERON_INSTANCE_RECONNECT_STREAMMGR_INTERVAL_SEC;

  // Number of attempts to connect to stream manager before giving up
  static const sp_string HERON_INSTANCE_RECONNECT_STREAMMGR_TIMES;

  // The queue capacity (num of items) in bolt for buffer packets to read from stream manager
  static const sp_string HERON_INSTANCE_INTERNAL_BOLT_READ_QUEUE_CAPACITY;

  // The queue capacity (num of items) in bolt for buffer packets to write to stream manager
  static const sp_string HERON_INSTANCE_INTERNAL_BOLT_WRITE_QUEUE_CAPACITY;

  // The queue capacity (num of items) in spout for buffer packets to read from stream manager
  static const sp_string HERON_INSTANCE_INTERNAL_SPOUT_READ_QUEUE_CAPACITY;

  // The queue capacity (num of items) in spout for buffer packets to write to stream manager
  static const sp_string HERON_INSTANCE_INTERNAL_SPOUT_WRITE_QUEUE_CAPACITY;

  // The maximum time in ms for an spout instance to emit tuples per attempt
  static const sp_string HERON_INSTANCE_EMIT_BATCH_TIME_MS;

  // The maximum number of bytes for n spout instance to emit tuples per attempt
  static const sp_string HERON_INSTANCE_EMIT_BATCH_SIZE;

  // The maximum # of data tuple to batch in a HeronDataTupleSet protobuf
  static const sp_string HERON_INSTANCE_SET_DATA_TUPLE_CAPACITY;

  // The maximum size in bytes of data tuple to batch in a HeronDataTupleSet protobuf
  static const sp_string HERON_INSTANCE_SET_DATA_TUPLE_SIZE_BYTES;

  // The maximum # of control tuple to batch in a HeronControlTupleSet protobuf
  static const sp_string HERON_INSTANCE_SET_CONTROL_TUPLE_CAPACITY;

  // For efficient acknowledgement
  static const sp_string HERON_INSTANCE_ACKNOWLEDGEMENT_NBUCKETS;
};
}  // namespace config
}  // namespace heron

#endif
