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

#include "proto/messages.h"

#include "config/heron-internals-config-vars.h"

namespace heron {
namespace config {

const sp_string HeronInternalsConfigVars::HERON_LOGGING_DIRECTORY = "heron.logging.directory";
const sp_string HeronInternalsConfigVars::HERON_LOGGING_MAXIMUM_SIZE_MB =
    "heron.logging.maximum.size.mb";
const sp_string HeronInternalsConfigVars::HERON_LOGGING_MAXIMUM_FILES =
    "heron.logging.maximum.files";
const sp_string HeronInternalsConfigVars::HERON_CHECK_TMANAGER_LOCATION_INTERVAL_SEC =
    "heron.check.tmanager.location.interval.sec";
const sp_string HeronInternalsConfigVars::HERON_LOGGING_PRUNE_INTERVAL_SEC =
    "heron.logging.prune.interval.sec";
const sp_string HeronInternalsConfigVars::HERON_LOGGING_FLUSH_INTERVAL_SEC =
    "heron.logging.flush.interval.sec";
const sp_string HeronInternalsConfigVars::HERON_LOGGING_ERR_THRESHOLD =
    "heron.logging.err.threshold";

const sp_string HeronInternalsConfigVars::HERON_METRICS_EXPORT_INTERVAL_SEC =
    "heron.metrics.export.interval.sec";

// heron.metricsmgr.* configs are for the metrics manager
const sp_string HeronInternalsConfigVars::HERON_METRICSMGR_SCRIBE_HOST =
    "heron.metricsmgr.scribe.host";
const sp_string HeronInternalsConfigVars::HERON_METRICSMGR_SCRIBE_PORT =
    "heron.metricsmgr.scribe.port";
const sp_string HeronInternalsConfigVars::HERON_METRICSMGR_SCRIBE_CATEGORY =
    "heron.metricsmgr.scribe.category";
const sp_string HeronInternalsConfigVars::HERON_METRICSMGR_SCRIBE_SERVICE_NAMESPACE =
    "heron.metricsmgr.scribe.service.namespace";
const sp_string HeronInternalsConfigVars::HERON_METRICSMGR_SCRIBE_WRITE_RETRY_TIMES =
    "heron.metricsmgr.scribe.write.retry.times";
const sp_string HeronInternalsConfigVars::HERON_METRICSMGR_SCRIBE_WRITE_TIMEOUT_SEC =
    "heron.metricsmgr.scribe.write.timeout.sec";
const sp_string HeronInternalsConfigVars::HERON_METRICSMGR_SCRIBE_PERIODIC_FLUSH_INTERVAL_SEC =
    "heron.metricsmgr.scribe.periodic.flush.interval.sec";
const sp_string HeronInternalsConfigVars::HERON_METRICSMGR_RECONNECT_TMANAGER_INTERVAL_SEC =
    "heron.metricsmgr.reconnect.tmanager.interval.sec";
const sp_string HeronInternalsConfigVars::HERON_METRICSMGR_NETWORK_OPTIONS_MAXIMUM_PACKET_MB =
    "heron.metricsmgr.network.options.maximum.packet.mb";

// heron.tmanager.* configs are for the metrics manager
const sp_string HeronInternalsConfigVars::HERON_TMANAGER_METRICS_COLLECTOR_MAXIMUM_INTERVAL_MIN =
    "heron.tmanager.metrics.collector.maximum.interval.min";
const sp_string HeronInternalsConfigVars::HERON_TMANAGER_ESTABLISH_RETRY_TIMES =
    "heron.tmanager.establish.retry.times";
const sp_string HeronInternalsConfigVars::HERON_TMANAGER_ESTABLISH_RETRY_INTERVAL_SEC =
    "heron.tmanager.establish.retry.interval.sec";
const sp_string HeronInternalsConfigVars::HERON_TMANAGER_NETWORK_SERVER_OPTIONS_MAXIMUM_PACKET_MB =
    "heron.tmanager.network.server.options.maximum.packet.mb";
const sp_string
    HeronInternalsConfigVars::HERON_TMANAGER_NETWORK_CONTROLLER_OPTIONS_MAXIMUM_PACKET_MB =
        "heron.tmanager.network.controller.options.maximum.packet.mb";
const sp_string HeronInternalsConfigVars::HERON_TMANAGER_NETWORK_STATS_OPTIONS_MAXIMUM_PACKET_MB =
    "heron.tmanager.network.stats.options.maximum.packet.mb";
const sp_string HeronInternalsConfigVars::HERON_TMANAGER_METRICS_COLLECTOR_PURGE_INTERVAL_SEC =
    "heron.tmanager.metrics.collector.purge.interval.sec";
const sp_string HeronInternalsConfigVars::HERON_TMANAGER_METRICS_COLLECTOR_MAXIMUM_EXCEPTION =
    "heron.tmanager.metrics.collector.maximum.exception";
const sp_string HeronInternalsConfigVars::HERON_TMANAGER_METRICS_NETWORK_BINDALLINTERFACES =
    "heron.tmanager.metrics.network.bindallinterfaces";
const sp_string HeronInternalsConfigVars::HERON_TMANAGER_STMGR_STATE_TIMEOUT_SEC =
    "heron.tmanager.stmgr.state.timeout.sec";

// heron.streammgr.* configs are for the stream manager
const sp_string HeronInternalsConfigVars::HERON_STREAMMGR_CACHE_DRAIN_FREQUENCY_MS =
    "heron.streammgr.cache.drain.frequency.ms";
const sp_string HeronInternalsConfigVars::HERON_STREAMMGR_CACHE_DRAIN_SIZE_MB =
    "heron.streammgr.cache.drain.size.mb";
const sp_string HeronInternalsConfigVars::HERON_STREAMMGR_MEMPOOL_MAX_MESSAGE_NUMBER =
    "heron.streammgr.mempool.max.message.number";
const sp_string HeronInternalsConfigVars::HERON_STREAMMGR_XORMGR_ROTATINGMAP_NBUCKETS =
    "heron.streammgr.xormgr.rotatingmap.nbuckets";
const sp_string HeronInternalsConfigVars::HERON_STREAMMGR_CLIENT_RECONNECT_INTERVAL_SEC =
    "heron.streammgr.client.reconnect.interval.sec";
const sp_string HeronInternalsConfigVars::HERON_STREAMMGR_CLIENT_RECONNECT_TMANAGER_INTERVAL_SEC =
    "heron.streammgr.client.reconnect.tmanager.interval.sec";
const sp_string HeronInternalsConfigVars::HERON_STREAMMGR_CLIENT_RECONNECT_TMANAGER_MAX_ATTEMPTS =
    "heron.streammgr.client.reconnect.tmanager.max.attempts";
const sp_string HeronInternalsConfigVars::HERON_STREAMMGR_NETWORK_OPTIONS_MAXIMUM_PACKET_MB =
    "heron.streammgr.network.options.maximum.packet.mb";
const sp_string HeronInternalsConfigVars::HERON_STREAMMGR_TMANAGER_HEARTBEAT_INTERVAL_SEC =
    "heron.streammgr.tmanager.heartbeat.interval.sec";
const sp_string HeronInternalsConfigVars::HERON_STREAMMGR_CONNECTION_READ_BATCH_SIZE_MB =
    "heron.streammgr.connection.read.batch.size.mb";
const sp_string HeronInternalsConfigVars::HERON_STREAMMGR_CONNECTION_WRITE_BATCH_SIZE_MB =
    "heron.streammgr.connection.write.batch.size.mb";
const sp_string HeronInternalsConfigVars::HERON_STREAMMGR_NETWORK_BACKPRESSURE_THRESHOLD =
    "heron.streammgr.network.backpressure.threshold";
const sp_string HeronInternalsConfigVars::HERON_STREAMMGR_NETWORK_BACKPRESSURE_HIGHWATERMARK_MB =
    "heron.streammgr.network.backpressure.highwatermark.mb";
const sp_string HeronInternalsConfigVars::HERON_STREAMMGR_NETWORK_BACKPRESSURE_LOWWATERMARK_MB =
    "heron.streammgr.network.backpressure.lowwatermark.mb";
const sp_string HeronInternalsConfigVars::HERON_STREAMMGR_STATEFUL_BUFFER_SIZE_MB =
    "heron.streammgr.stateful.buffer.size.mb";

// heron.instance.* configs are for the instance
const sp_string HeronInternalsConfigVars::HERON_INSTANCE_RECONNECT_STREAMMGR_INTERVAL_SEC =
    "heron.instance.reconnect.streammgr.interval.sec";
const sp_string HeronInternalsConfigVars::HERON_INSTANCE_RECONNECT_STREAMMGR_TIMES =
    "heron.instance.reconnect.streammgr.times";
const sp_string HeronInternalsConfigVars::HERON_INSTANCE_INTERNAL_BOLT_READ_QUEUE_CAPACITY =
    "heron.instance.internal.bolt.read.queue.capacity";
const sp_string HeronInternalsConfigVars::HERON_INSTANCE_INTERNAL_BOLT_WRITE_QUEUE_CAPACITY =
    "heron.instance.internal.bolt.write.queue.capacity";
const sp_string HeronInternalsConfigVars::HERON_INSTANCE_INTERNAL_SPOUT_READ_QUEUE_CAPACITY =
    "heron.instance.internal.spout.read.queue.capacity";
const sp_string HeronInternalsConfigVars::HERON_INSTANCE_INTERNAL_SPOUT_WRITE_QUEUE_CAPACITY =
    "heron.instance.internal.spout.write.queue.capacity";
const sp_string HeronInternalsConfigVars::HERON_INSTANCE_EMIT_BATCH_TIME_MS =
    "heron.instance.emit.batch.time.ms";
const sp_string HeronInternalsConfigVars::HERON_INSTANCE_EMIT_BATCH_SIZE =
    "heron.instance.emit.batch.size.bytes";
const sp_string HeronInternalsConfigVars::HERON_INSTANCE_SET_DATA_TUPLE_CAPACITY =
    "heron.instance.set.data.tuple.capacity";
const sp_string HeronInternalsConfigVars::HERON_INSTANCE_SET_DATA_TUPLE_SIZE_BYTES =
    "heron.instance.set.data.tuple.size.bytes";
const sp_string HeronInternalsConfigVars::HERON_INSTANCE_SET_CONTROL_TUPLE_CAPACITY =
    "heron.instance.set.control.tuple.capacity";
const sp_string HeronInternalsConfigVars::HERON_INSTANCE_ACKNOWLEDGEMENT_NBUCKETS =
    "heron.instance.acknowledgement.nbuckets";
}  // namespace config
}  // namespace heron
