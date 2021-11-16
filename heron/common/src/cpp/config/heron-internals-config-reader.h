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
// heron-internals-config-reader.h
//
// This file deals with default values for heron-internals config
// variables. It takes in a config file to load the defaults
// It is a singleton so the whole process could access it
//
///////////////////////////////////////////////////////////////
#ifndef HERON_INTERNALS_CONFIG_READER_H
#define HERON_INTERNALS_CONFIG_READER_H
#include "basics/sptypes.h"
#include "config/yaml-file-reader.h"

class EventLoop;

namespace heron {
namespace config {

class HeronInternalsConfigReader : public YamlFileReader {
 public:
  // Return the singleton if there is one,
  // or NULL if there is not.
  static HeronInternalsConfigReader* Instance();
  // Check whether the singleton is created or not
  static bool Exists();
  // Create a singleton reader from a config file,
  // which will check and reload the config change
  static void Create(std::shared_ptr<EventLoop> eventLoop,
                     const sp_string& _defaults_file, const sp_string& _override_file);
  // Create a singleton reader from a config file,
  // which will not check or reload the config change
  static void Create(const sp_string& _defaults_file, const sp_string& _override_file);

  virtual void OnConfigFileLoad();

  /**
  * Heron common Config Getters
  **/
  // The relative path to the logging directory
  sp_string GetHeronLoggingDirectory();

  // The maximum log file size in MB
  sp_int32 GetHeronLoggingMaximumSizeMb();

  // The maximum number of log files
  sp_int32 GetHeronLoggingMaximumFiles();

  // The interval after which we check if the tmanager location
  // has been set or not
  sp_int32 GetCheckTManagerLocationIntervalSec();

  // The interval in seconds to prune logging files in C+++
  sp_int32 GetHeronLoggingPruneIntervalSec();

  // The interval in seconds to flush log files in C+++
  sp_int32 GetHeronLoggingFlushIntervalSec();

  // The threadhold level to log error
  sp_int32 GetHeronLoggingErrThreshold();

  // The interval in seconds for different components to export metrics to metrics manager
  sp_int32 GetHeronMetricsExportIntervalSec();

  /**
  * Metrics Manager Config Getters
  **/
  // The host of scribe to be exported metrics to
  sp_string GetHeronMetricsmgrScribeHost();

  // The port of scribe to be exported metrics to
  sp_int32 GetHeronMetricsmgrScribePort();

  // The category of the scribe to be exported metrics to
  sp_string GetHeronMetricsmgrScribeCategory();

  // The service name of the metrics in cuckoo_json
  sp_string GetHeronMetricsmgrScribeServiceNamespace();

  // The maximum retry attempts to write metrics to scribe
  sp_int32 GetHeronMetricsmgrScribeWriteRetryTimes();

  // The timeout in seconds for metrics manager to write metrics to scribe
  sp_int32 GetHeronMetricsmgrScribeWriteTimeoutSec();

  // The interval in seconds to flush cached metircs to scribe
  sp_int32 GetHeronMetricsmgrScribePeriodicFlushIntervalSec();

  // The interval in seconds to reconnect to tmanager if a connection failure happens
  sp_int32 GetHeronMetricsmgrReconnectTmanagerIntervalSec();

  // The maximum packet size in MB of metrics manager's network options
  sp_int32 GetHeronMetricsmgrNetworkOptionsMaximumPacketMb();

  /**
  * Tmanager Config Getters
  **/
  // The maximum interval in minutes of metrics to be kept in tmanager
  sp_int32 GetHeronTmanagerMetricsCollectorMaximumIntervalMin();

  // The maximum time to retry to establish the tmanager
  sp_int32 GetHeronTmanagerEstablishRetryTimes();

  // The interval to retry to establish the tmanager
  sp_int32 GetHeronTmanagerEstablishRetryIntervalSec();

  // The maximum packet size in MB of tmanager's network options for stmgrs to connect to
  sp_int32 GetHeronTmanagerNetworkServerOptionsMaximumPacketMb();

  // The maximum packet size in MB of tmanager's network options for scheduler to connect to
  sp_int32 GetHeronTmanagerNetworkControllerOptionsMaximumPacketMb();

  // The maximum packet size in MB of tmanager's network options for stat queries
  sp_int32 GetHeronTmanagerNetworkStatsOptionsMaximumPacketMb();

  // The inteval for tmanager to purge metrics from socket
  sp_int32 GetHeronTmanagerMetricsCollectorPurgeIntervalSec();

  // The maximum # of exception to be stored in tmetrics collector, to prevent potential OOM
  sp_int32 GetHeronTmanagerMetricsCollectorMaximumException();

  // Should metrics server bind on all interfaces
  bool GetHeronTmanagerMetricsNetworkBindAllInterfaces();

  // The timeout in seconds for stream mgr, compared with (current time - last heartbeat time)
  sp_int32 GetHeronTmanagerStmgrStateTimeoutSec();

  /**
  * Stream manager Config Getters
  **/
  // The frequency in ms to drain the tuple cache in stream manager
  sp_int32 GetHeronStreammgrCacheDrainFrequencyMs();

  // The sized based threshold in MB for draining the tuple cache
  sp_int32 GetHeronStreammgrCacheDrainSizeMb();

  // The size based threshold in MB for buffering data tuples waiting for
  // checkpoint markers to arrive before giving up
  sp_int32 GetHeronStreammgrStatefulBufferSizeMb();

  // The max number of messages in the memory pool for each message type
  sp_int32 GetHeronStreammgrMempoolMaxMessageNumber();

  // Get the Nbucket value, for efficient acknowledgement
  sp_int32 GetHeronStreammgrXormgrRotatingmapNbuckets();

  // The reconnect interval to other stream managers in second for stream manager client
  sp_int32 GetHeronStreammgrClientReconnectIntervalSec();

  // The reconnect interval to tamster in second for stream manager client
  sp_int32 GetHeronStreammgrClientReconnectTmanagerIntervalSec();

  // The max reconnect attempts to tmanager for stream manager client
  sp_int32 GetHeronStreammgrClientReconnectTmanagerMaxAttempts();

  // The maximum packet size in MB of stream manager's network options
  sp_int32 GetHeronStreammgrNetworkOptionsMaximumPacketMb();

  // The interval in seconds to send heartbeat
  sp_int32 GetHeronStreammgrTmanagerHeartbeatIntervalSec();

  // Maximum batch size in MB to read by stream manager from socket
  sp_int32 GetHeronStreammgrConnectionReadBatchSizeMb();

  // Maximum batch size in MB to write by stream manager to socket
  sp_int32 GetHeronStreammgrConnectionWriteBatchSizeMb();

  // Number of times we should wait to see a buffer full while enqueueing data before declaring
  // start of back pressure
  sp_int32 GetHeronStreammgrNetworkBackpressureThreshold();

  // High water mark on the num in MB that can be left outstanding on a connection
  sp_int32 GetHeronStreammgrNetworkBackpressureHighwatermarkMb();

  // Low water mark on the num in MB that can be left outstanding on a connection
  sp_int32 GetHeronStreammgrNetworkBackpressureLowwatermarkMb();

  /**
  * Instance Config Getters
  **/

  // Interval in seconds to reconnect to the stream manager
  int GetHeronInstanceReconnectStreammgrIntervalSec();

  // Number of attempts to connect to stream manager before giving up
  int GetHeronInstanceReconnectStreammgrTimes();

  // The queue capacity (num of items) in bolt for buffer packets to read from stream manager
  int GetHeronInstanceInternalBoltReadQueueCapacity();

  // The queue capacity (num of items) in bolt for buffer packets to write to stream manager
  int GetHeronInstanceInternalBoltWriteQueueCapacity();

  // The queue capacity (num of items) in spout for buffer packets to read from stream manager
  int GetHeronInstanceInternalSpoutReadQueueCapacity();

  // The queue capacity (num of items) in spout for buffer packets to write to stream manager
  int GetHeronInstanceInternalSpoutWriteQueueCapacity();

  // The maximum time in ms for an spout instance to emit tuples per attempt
  int GetHeronInstanceEmitBatchTimeMs();

  // The maximum number of bytes for an spout instance to emit tuples per attempt
  int GetHeronInstanceEmitBatchSize();

  // The maximum # of data tuple to batch in a HeronDataTupleSet protobuf
  int GetHeronInstanceSetDataTupleCapacity();

  // The maximum size in bytes of data tuple to batch in a HeronDataTupleSet protobuf
  int GetHeronInstanceSetDataTupleSizeBytes();

  // The maximum # of control tuple to batch in a HeronControlTupleSet protobuf
  int GetHeronInstanceSetControlTupleCapacity();

  // For efficient acknowledgement
  int GetHeronInstanceAcknowledgementNbuckets();

 protected:
  HeronInternalsConfigReader(std::shared_ptr<EventLoop> eventLoop,
                             const sp_string& _defaults_file,
                             const sp_string& _override_file);
  virtual ~HeronInternalsConfigReader();
  sp_string override_file_;
  void LoadOverrideConfig();
  static HeronInternalsConfigReader* heron_internals_config_reader_;
};
}  // namespace config
}  // namespace heron

#endif
