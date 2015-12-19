#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "config/metrics-sinks-vars.h"

namespace heron { namespace config {

const sp_string MetricsSinksVars::METRICS_SINKS_TMASTER_SINK = "tmaster-sink";
const sp_string MetricsSinksVars::METRICS_SINKS_TMASTER_METRICS = "tmaster-metrics-type";

}} // end namespace
