#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "config/cluster-config-vars.h"

namespace heron { namespace config {

const sp_string ClusterConfigVars::CLUSTER_METRICS_INTERVAL = "cluster.metrics.collection.interval";

}} // end namespace
