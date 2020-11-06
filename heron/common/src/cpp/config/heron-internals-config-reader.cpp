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

#include "config/heron-internals-config-reader.h"
#include <string>
#include "config/heron-internals-config-vars.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "proto/messages.h"
#include "network/network.h"
#include "threads/threads.h"

#include "yaml-cpp/yaml.h"

namespace heron {
namespace config {

// Global initialization to facilitate singleton design pattern
HeronInternalsConfigReader* HeronInternalsConfigReader::heron_internals_config_reader_ = 0;

HeronInternalsConfigReader::HeronInternalsConfigReader(std::shared_ptr<EventLoop> eventLoop,
                                                       const sp_string& _defaults_file,
                                                       const sp_string& _override_file)
    : YamlFileReader(eventLoop, _defaults_file) {
  override_file_ = _override_file;
  LoadConfig();
  LoadOverrideConfig();
}

HeronInternalsConfigReader::~HeronInternalsConfigReader() { delete heron_internals_config_reader_; }

HeronInternalsConfigReader* HeronInternalsConfigReader::Instance() {
  if (heron_internals_config_reader_ == 0) {
    LOG(FATAL) << "Singleton HeronInternalsConfigReader has not been created";
  }

  return heron_internals_config_reader_;
}

bool HeronInternalsConfigReader::Exists() {
  return (heron_internals_config_reader_ != NULL);  // Return true/false
}

void HeronInternalsConfigReader::Create(std::shared_ptr<EventLoop> eventLoop,
                                        const sp_string& _defaults_file,
                                        const sp_string& _override_file) {
  if (heron_internals_config_reader_) {
    LOG(FATAL) << "Singleton HeronInternalsConfigReader has already been created";
  } else {
    heron_internals_config_reader_ =
      new HeronInternalsConfigReader(eventLoop, _defaults_file, _override_file);
  }
}

void HeronInternalsConfigReader::Create(const sp_string& _defaults_file,
                                        const sp_string& _override_file) {
  Create(NULL, _defaults_file, _override_file);
}

void HeronInternalsConfigReader::LoadOverrideConfig() {
  if (override_file_.empty()) {
    return;
  }
  YAML::Node override_config = YAML::LoadFile(override_file_);
  for (auto n : config_) {
    if (n.first.IsScalar()) {
      const std::string& key = n.first.Scalar();
      auto override_val = YAML::Node(override_config[key]);
      if (override_val) {
        LOG(INFO) << "Add overriding configuration '" << key << "'";
        config_[n.first] = override_val;
      }
    }
  }
}
void HeronInternalsConfigReader::OnConfigFileLoad() {
  // Nothing really
}

sp_string HeronInternalsConfigReader::GetHeronLoggingDirectory() {
  return config_[HeronInternalsConfigVars::HERON_LOGGING_DIRECTORY].as<std::string>();
}

sp_int32 HeronInternalsConfigReader::GetHeronLoggingMaximumSizeMb() {
  return config_[HeronInternalsConfigVars::HERON_LOGGING_MAXIMUM_SIZE_MB].as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronLoggingMaximumFiles() {
  return config_[HeronInternalsConfigVars::HERON_LOGGING_MAXIMUM_FILES].as<int>();
}

sp_int32 HeronInternalsConfigReader::GetCheckTManagerLocationIntervalSec() {
  return config_[HeronInternalsConfigVars::HERON_CHECK_TMANAGER_LOCATION_INTERVAL_SEC].as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronLoggingPruneIntervalSec() {
  return config_[HeronInternalsConfigVars::HERON_LOGGING_PRUNE_INTERVAL_SEC].as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronLoggingFlushIntervalSec() {
  return config_[HeronInternalsConfigVars::HERON_LOGGING_FLUSH_INTERVAL_SEC].as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronLoggingErrThreshold() {
  return config_[HeronInternalsConfigVars::HERON_LOGGING_ERR_THRESHOLD].as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronMetricsExportIntervalSec() {
  return config_[HeronInternalsConfigVars::HERON_METRICS_EXPORT_INTERVAL_SEC].as<int>();
}

/**
* Followings are getters for Metrics Manager Configurations
**/
sp_string HeronInternalsConfigReader::GetHeronMetricsmgrScribeHost() {
  return config_[HeronInternalsConfigVars::HERON_METRICSMGR_SCRIBE_HOST].as<std::string>();
}

sp_int32 HeronInternalsConfigReader::GetHeronMetricsmgrScribePort() {
  return config_[HeronInternalsConfigVars::HERON_METRICSMGR_SCRIBE_PORT].as<int>();
}

sp_string HeronInternalsConfigReader::GetHeronMetricsmgrScribeCategory() {
  return config_[HeronInternalsConfigVars::HERON_METRICSMGR_SCRIBE_CATEGORY].as<std::string>();
}

sp_string HeronInternalsConfigReader::GetHeronMetricsmgrScribeServiceNamespace() {
  return config_[HeronInternalsConfigVars::HERON_METRICSMGR_SCRIBE_SERVICE_NAMESPACE]
      .as<std::string>();
}

sp_int32 HeronInternalsConfigReader::GetHeronMetricsmgrScribeWriteRetryTimes() {
  return config_[HeronInternalsConfigVars::HERON_METRICSMGR_SCRIBE_WRITE_RETRY_TIMES].as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronMetricsmgrScribeWriteTimeoutSec() {
  return config_[HeronInternalsConfigVars::HERON_METRICSMGR_SCRIBE_WRITE_TIMEOUT_SEC].as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronMetricsmgrScribePeriodicFlushIntervalSec() {
  return config_[HeronInternalsConfigVars::HERON_METRICSMGR_SCRIBE_PERIODIC_FLUSH_INTERVAL_SEC]
      .as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronMetricsmgrReconnectTmanagerIntervalSec() {
  return config_[HeronInternalsConfigVars::HERON_METRICSMGR_RECONNECT_TMANAGER_INTERVAL_SEC]
      .as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronMetricsmgrNetworkOptionsMaximumPacketMb() {
  return config_[HeronInternalsConfigVars::HERON_METRICSMGR_NETWORK_OPTIONS_MAXIMUM_PACKET_MB]
      .as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronTmanagerMetricsCollectorMaximumIntervalMin() {
  return config_[HeronInternalsConfigVars::HERON_TMANAGER_METRICS_COLLECTOR_MAXIMUM_INTERVAL_MIN]
      .as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronTmanagerEstablishRetryTimes() {
  return config_[HeronInternalsConfigVars::HERON_TMANAGER_ESTABLISH_RETRY_TIMES].as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronTmanagerEstablishRetryIntervalSec() {
  return config_[HeronInternalsConfigVars::HERON_TMANAGER_ESTABLISH_RETRY_INTERVAL_SEC].as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronTmanagerNetworkServerOptionsMaximumPacketMb() {
  return config_[HeronInternalsConfigVars::HERON_TMANAGER_NETWORK_SERVER_OPTIONS_MAXIMUM_PACKET_MB]
      .as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronTmanagerNetworkControllerOptionsMaximumPacketMb() {
  return config_
      [HeronInternalsConfigVars::HERON_TMANAGER_NETWORK_CONTROLLER_OPTIONS_MAXIMUM_PACKET_MB]
          .as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronTmanagerNetworkStatsOptionsMaximumPacketMb() {
  return config_[HeronInternalsConfigVars::HERON_TMANAGER_NETWORK_STATS_OPTIONS_MAXIMUM_PACKET_MB]
      .as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronTmanagerMetricsCollectorPurgeIntervalSec() {
  return config_[HeronInternalsConfigVars::HERON_TMANAGER_METRICS_COLLECTOR_PURGE_INTERVAL_SEC]
      .as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronTmanagerMetricsCollectorMaximumException() {
  return config_[HeronInternalsConfigVars::HERON_TMANAGER_METRICS_COLLECTOR_MAXIMUM_EXCEPTION]
      .as<int>();
}

bool HeronInternalsConfigReader::GetHeronTmanagerMetricsNetworkBindAllInterfaces() {
  return config_[HeronInternalsConfigVars::HERON_TMANAGER_METRICS_NETWORK_BINDALLINTERFACES]
      .as<bool>();
}

sp_int32 HeronInternalsConfigReader::GetHeronTmanagerStmgrStateTimeoutSec() {
  return config_[HeronInternalsConfigVars::HERON_TMANAGER_STMGR_STATE_TIMEOUT_SEC].as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronStreammgrCacheDrainFrequencyMs() {
  return config_[HeronInternalsConfigVars::HERON_STREAMMGR_CACHE_DRAIN_FREQUENCY_MS].as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronStreammgrCacheDrainSizeMb() {
  return config_[HeronInternalsConfigVars::HERON_STREAMMGR_CACHE_DRAIN_SIZE_MB].as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronStreammgrStatefulBufferSizeMb() {
  return config_[HeronInternalsConfigVars::HERON_STREAMMGR_STATEFUL_BUFFER_SIZE_MB]
      .as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronStreammgrMempoolMaxMessageNumber() {
  return config_[HeronInternalsConfigVars::HERON_STREAMMGR_MEMPOOL_MAX_MESSAGE_NUMBER].as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronStreammgrXormgrRotatingmapNbuckets() {
  return config_[HeronInternalsConfigVars::HERON_STREAMMGR_XORMGR_ROTATINGMAP_NBUCKETS].as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronStreammgrClientReconnectTmanagerMaxAttempts() {
  return config_[HeronInternalsConfigVars::HERON_STREAMMGR_CLIENT_RECONNECT_TMANAGER_MAX_ATTEMPTS]
      .as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronStreammgrClientReconnectIntervalSec() {
  return config_[HeronInternalsConfigVars::HERON_STREAMMGR_CLIENT_RECONNECT_INTERVAL_SEC].as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronStreammgrClientReconnectTmanagerIntervalSec() {
  return config_[HeronInternalsConfigVars::HERON_STREAMMGR_CLIENT_RECONNECT_TMANAGER_INTERVAL_SEC]
      .as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronStreammgrNetworkOptionsMaximumPacketMb() {
  return config_[HeronInternalsConfigVars::HERON_STREAMMGR_NETWORK_OPTIONS_MAXIMUM_PACKET_MB]
      .as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronStreammgrTmanagerHeartbeatIntervalSec() {
  return config_[HeronInternalsConfigVars::HERON_STREAMMGR_TMANAGER_HEARTBEAT_INTERVAL_SEC]
      .as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronStreammgrConnectionReadBatchSizeMb() {
  return config_[HeronInternalsConfigVars::HERON_STREAMMGR_CONNECTION_READ_BATCH_SIZE_MB].as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronStreammgrConnectionWriteBatchSizeMb() {
  return config_[HeronInternalsConfigVars::HERON_STREAMMGR_CONNECTION_WRITE_BATCH_SIZE_MB]
      .as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronStreammgrNetworkBackpressureThreshold() {
  return config_[HeronInternalsConfigVars::HERON_STREAMMGR_NETWORK_BACKPRESSURE_THRESHOLD]
      .as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronStreammgrNetworkBackpressureHighwatermarkMb() {
  return config_[HeronInternalsConfigVars::HERON_STREAMMGR_NETWORK_BACKPRESSURE_HIGHWATERMARK_MB]
      .as<int>();
}

sp_int32 HeronInternalsConfigReader::GetHeronStreammgrNetworkBackpressureLowwatermarkMb() {
  return config_[HeronInternalsConfigVars::HERON_STREAMMGR_NETWORK_BACKPRESSURE_LOWWATERMARK_MB]
      .as<int>();
}

int HeronInternalsConfigReader::GetHeronInstanceReconnectStreammgrIntervalSec() {
  return config_[HeronInternalsConfigVars::HERON_INSTANCE_RECONNECT_STREAMMGR_INTERVAL_SEC]
      .as<int>();
}

int HeronInternalsConfigReader::GetHeronInstanceReconnectStreammgrTimes() {
  return config_[HeronInternalsConfigVars::HERON_INSTANCE_RECONNECT_STREAMMGR_TIMES]
      .as<int>();
}

int HeronInternalsConfigReader::GetHeronInstanceInternalBoltReadQueueCapacity() {
  return config_[HeronInternalsConfigVars::HERON_INSTANCE_INTERNAL_BOLT_READ_QUEUE_CAPACITY]
      .as<int>();
}

int HeronInternalsConfigReader::GetHeronInstanceInternalBoltWriteQueueCapacity() {
  return config_[HeronInternalsConfigVars::HERON_INSTANCE_INTERNAL_BOLT_WRITE_QUEUE_CAPACITY]
      .as<int>();
}

int HeronInternalsConfigReader::GetHeronInstanceInternalSpoutReadQueueCapacity() {
  return config_[HeronInternalsConfigVars::HERON_INSTANCE_INTERNAL_SPOUT_READ_QUEUE_CAPACITY]
      .as<int>();
}

int HeronInternalsConfigReader::GetHeronInstanceInternalSpoutWriteQueueCapacity() {
  return config_[HeronInternalsConfigVars::HERON_INSTANCE_INTERNAL_SPOUT_WRITE_QUEUE_CAPACITY]
      .as<int>();
}

int HeronInternalsConfigReader::GetHeronInstanceEmitBatchTimeMs() {
  return config_[HeronInternalsConfigVars::HERON_INSTANCE_EMIT_BATCH_TIME_MS]
      .as<int>();
}

int HeronInternalsConfigReader::GetHeronInstanceEmitBatchSize() {
  return config_[HeronInternalsConfigVars::HERON_INSTANCE_EMIT_BATCH_SIZE]
      .as<int>();
}

int HeronInternalsConfigReader::GetHeronInstanceSetDataTupleCapacity() {
  return config_[HeronInternalsConfigVars::HERON_INSTANCE_SET_DATA_TUPLE_CAPACITY]
      .as<int>();
}

int HeronInternalsConfigReader::GetHeronInstanceSetDataTupleSizeBytes() {
  return config_[HeronInternalsConfigVars::HERON_INSTANCE_SET_DATA_TUPLE_SIZE_BYTES]
      .as<int>();
}

int HeronInternalsConfigReader::GetHeronInstanceSetControlTupleCapacity() {
  return config_[HeronInternalsConfigVars::HERON_INSTANCE_SET_CONTROL_TUPLE_CAPACITY]
      .as<int>();
}

int HeronInternalsConfigReader::GetHeronInstanceAcknowledgementNbuckets() {
  return config_[HeronInternalsConfigVars::HERON_INSTANCE_ACKNOWLEDGEMENT_NBUCKETS]
      .as<int>();
}

}  // namespace config
}  // namespace heron
