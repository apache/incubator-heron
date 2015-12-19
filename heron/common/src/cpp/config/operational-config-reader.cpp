#include "yaml-cpp/yaml.h"
#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "config/yaml-file-reader.h"
#include "config/operational-config-vars.h"
#include "config/operational-config-reader.h"

namespace heron { namespace config {

OperationalConfigReader::OperationalConfigReader(EventLoop* eventLoop,
                                                 const sp_string& _defaults_file)
  : YamlFileReader(eventLoop, _defaults_file)
{
  LoadConfig();
}

OperationalConfigReader::~OperationalConfigReader()
{
}

sp_string OperationalConfigReader::GetTopologyReleaseOverride(const sp_string& _top_name)
{
  if (config_[OperationalConfigVars::TOPOLOGY_RELEASE_OVERRIDES]) {
    YAML::Node n = config_[OperationalConfigVars::TOPOLOGY_RELEASE_OVERRIDES];
    if (n.IsMap() && n[_top_name]) {
      return n[_top_name].as<sp_string>();
    } else {
      return "";
    }
  } else {
      return "";
  }
}

void OperationalConfigReader::OnConfigFileLoad()
{
  // Nothing really
}

}} // end namespace
