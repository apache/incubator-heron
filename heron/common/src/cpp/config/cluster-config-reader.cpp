#include "yaml-cpp/yaml.h"
#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "config/yaml-file-reader.h"
#include "config/cluster-config-vars.h"
#include "config/cluster-config-reader.h"

namespace heron { namespace config {

ClusterConfigReader::ClusterConfigReader(EventLoop* eventLoop,
                                         const sp_string& _defaults_file)
  : YamlFileReader(eventLoop, _defaults_file)
{
  LoadConfig();
}

ClusterConfigReader::~ClusterConfigReader()
{
}

void ClusterConfigReader::FillClusterConfig(proto::api::Topology* _topology)
{
  // Fill in the cluster config
  for (YAML::const_iterator iter = config_.begin();
       iter != config_.end(); ++iter) {
    proto::api::Config::KeyValue* kv = _topology->mutable_topology_config()->add_kvs();
    kv->set_key(iter->first.as<sp_string>());
    kv->set_value(iter->second.as<sp_string>());
  }
}

void ClusterConfigReader::OnConfigFileLoad()
{
  AddIfMissing(ClusterConfigVars::CLUSTER_METRICS_INTERVAL, "60");
}

}} // end namespace
