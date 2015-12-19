#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "config/topology-config-vars.h"

namespace heron { namespace config {

const sp_string TopologyConfigVars::TOPOLOGY_DEBUG = "topology.debug";
const sp_string TopologyConfigVars::TOPOLOGY_STMGRS = "topology.stmgrs";
const sp_string TopologyConfigVars::TOPOLOGY_MESSAGE_TIMEOUT_SECS = "topology.message.timeout.secs";
const sp_string TopologyConfigVars::TOPOLOGY_WORKER_CHILDOPTS = "topology.worker.childopts";
const sp_string TopologyConfigVars::TOPOLOGY_COMPONENT_PARALLELISM = "topology.component.parallelism";
const sp_string TopologyConfigVars::TOPOLOGY_MAX_SPOUT_PENDING = "topology.max.spout.pending";
const sp_string TopologyConfigVars::TOPOLOGY_SERIALIZER_CLASSNAME = "topology.serializer.classname";
const sp_string TopologyConfigVars::TOPOLOGY_TICK_TUPLE_FREQ_SECS = "topology.tick.tuple.freq.secs";
const sp_string TopologyConfigVars::TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS = "topology.enable.message.timeouts";
const sp_string TopologyConfigVars::TOPOLOGY_ENABLE_ACKING = "topology.acking";
const sp_string TopologyConfigVars::TOPOLOGY_CONTAINER_CPU_REQUESTED = "topology.container.cpu";
const sp_string TopologyConfigVars::TOPOLOGY_CONTAINER_RAM_REQUESTED = "topology.container.ram";
const sp_string TopologyConfigVars::TOPOLOGY_NAME = "topology.name";
const sp_string TopologyConfigVars::TOPOLOGY_TEAM_NAME = "topology.team.name";
const sp_string TopologyConfigVars::TOPOLOGY_TEAM_EMAIL = "topology.team.email";
const sp_string TopologyConfigVars::TOPOLOGY_CAP_TICKET = "topology.cap.ticket";
const sp_string TopologyConfigVars::TOPOLOGY_PROJECT_NAME = "topology.project.name";

}} // end namespace
