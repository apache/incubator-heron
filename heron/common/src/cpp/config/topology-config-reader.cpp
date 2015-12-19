#include "yaml-cpp/yaml.h"
#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "config/yaml-file-reader.h"
#include "config/topology-config-vars.h"
#include "config/topology-config-reader.h"

#include <set>

namespace heron { namespace config {

TopologyConfigReader::TopologyConfigReader(EventLoop* eventLoop,
                                           const sp_string& _defaults_file)
  : YamlFileReader(eventLoop, _defaults_file)
{
  LoadConfig();
}

TopologyConfigReader::~TopologyConfigReader()
{
}

void TopologyConfigReader::BackFillTopologyConfig(proto::api::Topology* _topology)
{
  // Construct a temporary set
  std::set<sp_string> topology_config;
  if (_topology->has_topology_config()) {
    const proto::api::Config& cfg = _topology->topology_config();
    for (sp_int32 i = 0; i < cfg.kvs_size(); ++i) {
      topology_config.insert(cfg.kvs(i).key());
    }
  }

  // Fill in the user variables
  for (YAML::const_iterator iter = config_.begin();
       iter != config_.end(); ++iter) {
    if (topology_config.find(iter->first.as<sp_string>()) == topology_config.end()) {
      // We need to backfill this variable
      proto::api::Config::KeyValue* kv = _topology->mutable_topology_config()->add_kvs();
      kv->set_key(iter->first.as<sp_string>());
      kv->set_value(iter->second.as<sp_string>());
    }
  }
}

void TopologyConfigReader::OnConfigFileLoad()
{
  AddIfMissing(TopologyConfigVars::TOPOLOGY_DEBUG, "false");
  AddIfMissing(TopologyConfigVars::TOPOLOGY_STMGRS, "1");
  AddIfMissing(TopologyConfigVars::TOPOLOGY_MESSAGE_TIMEOUT_SECS, "30");
  AddIfMissing(TopologyConfigVars::TOPOLOGY_COMPONENT_PARALLELISM, "1");
  AddIfMissing(TopologyConfigVars::TOPOLOGY_MAX_SPOUT_PENDING, "100");
  AddIfMissing(TopologyConfigVars::TOPOLOGY_ENABLE_ACKING, "false");
  AddIfMissing(TopologyConfigVars::TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, "true");
}

}} // end namespace
