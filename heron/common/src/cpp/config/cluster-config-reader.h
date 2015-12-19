#ifndef CLUSTER_CONFIG_READER_H_
#define CLUSTER_CONFIG_READER_H_

namespace heron { namespace config {

class ClusterConfigReader : public YamlFileReader
{
 public:
  ClusterConfigReader(EventLoop* eventLoop,
                      const sp_string& _defaults_file);
  virtual ~ClusterConfigReader();

  // Fill topology config with cluster config
  void FillClusterConfig(proto::api::Topology* _topology);

  virtual void OnConfigFileLoad();
};

}} // end namespace

#endif
