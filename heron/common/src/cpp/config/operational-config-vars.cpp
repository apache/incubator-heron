#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "config/operational-config-vars.h"

namespace heron { namespace config {

const sp_string OperationalConfigVars::TOPOLOGY_RELEASE_OVERRIDES = "operational.topology.release.overrides";

}} // end namespace
