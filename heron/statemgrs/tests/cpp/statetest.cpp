#include <iostream>

#include "common/common.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "heron-zkstatemgr.h"
#include <string>

using heron::common::HeronStateMgr;

void TMasterLocationWatchHandler()
{
  std::cout << "TMasterLocationWatchHandler triggered " << std::endl;
}

int main(int argc, char* argv[])
{
  /*
   * This is not a test per se, just comes handy when trying out zookeeper changes.
   * It needs to be run with a local zookeeper.
   * To trigger a session expiry, a partition needs to be created
   * Create partition,
   * sudo ipfw add 00993 deny tcp from any to any dst-port 2181
   * Delete partition
   * sudo ipfw delete 00993
   * Console logs need to monitored to verify the behavior on seesion expiry.
   */
  EventLoopImpl ss;

  if (argc < 3) {
    std::cout << "Usage: " << argv[0] << " <hostportlist> <topleveldir> " << std::endl;
    exit(1);
  }

  const std::string host_port = argv[1];
  const std::string top_level_dir = argv[2];
  const std::string topology_name = "test_topology";

  HeronStateMgr* state_mgr = HeronStateMgr::MakeStateMgr(host_port, top_level_dir, &ss);
  state_mgr->SetTMasterLocationWatch(topology_name,
    [] () { TMasterLocationWatchHandler(); } );
  ss.loop();
  return 0;
}
