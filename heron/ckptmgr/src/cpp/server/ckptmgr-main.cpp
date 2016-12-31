
#include <iostream>
#include "ckptmanager/ckptmgr.h"
#include "basics/basics.h"
#include "network/network.h"
#include "proto/messages.h"

int main(int argc, char* argv[]) {
  if (argc != 5) {
    std::cout << "Usage: " << argv[0] << " "
              << "<topname> <topid> <ckptmgr_id> <myport>"
              << std::endl;
    ::exit(1);
  }

  std::string topology_name = argv[1];
  std::string topology_id = argv[2];
  std::string ckptmgr_id = argv[3];
  sp_int32 my_port = atoi(argv[4]);
  EventLoopImpl ss;

  heron::ckptmgr::CkptMgr mgr(&ss, my_port, topology_name, topology_id, ckptmgr_id);

  mgr.Init();
  ss.loop();
  return 0;
}
