#include <iostream>

#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "config/heron-internals-config-reader.h"

#include "manager/tmaster.h"

int main(int argc, char* argv[])
{
  if (argc != 11) {
    std::cout
      << "Usage: " << argv[0] << " "
      << "<controller-port> <master-port> <stats-port> "
      << "<topology_name> <topology_id> <zk_hostportlist> "
      << "<topdir> <sgmr1,...> <heron_internals_config_filename> <metrics-manager-port>"
      << std::endl;
    std::cout << "If zk_hostportlist is empty please say LOCALMODE\n";
    ::exit(1);
  }

  sp_string myhost = IpUtils::getHostName();
  sp_int32 controller_port = atoi(argv[1]);
  sp_int32 master_port = atoi(argv[2]);
  sp_int32 stats_port = atoi(argv[3]);
  sp_string topology_name = argv[4];
  sp_string topology_id = argv[5];
  sp_string zkhostportlist = argv[6];
  if (zkhostportlist == "LOCALMODE") {
    zkhostportlist = "";
  }
  sp_string topdir = argv[7];
  std::vector<std::string> stmgrs = StrUtils::split(argv[8], ",");
  sp_string heron_internals_config_filename = argv[9];
  sp_int32 metrics_manager_port = atoi(argv[10]);

  EventLoopImpl ss;

  // Read heron internals config from local file
  // Create the heron-internals-config-reader to read the heron internals config
  heron::config::HeronInternalsConfigReader::Create(&ss, heron_internals_config_filename);

  heron::common::Initialize(argv[0], topology_id.c_str());

  LOG(INFO) << "Starting tmaster for topology "
            << topology_name << " with topology id "
            << topology_id << " zkhostport "
            << zkhostportlist << " zkroot "
       << topdir << " and nstmgrs " << stmgrs.size() << std::endl;

  heron::tmaster::TMaster tmaster(zkhostportlist, topology_name,
                                  topology_id, topdir, stmgrs,
                                  controller_port,
                                  master_port, stats_port, metrics_manager_port, myhost, &ss);
  ss.loop();
  return 0;
}
