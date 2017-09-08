#include <iostream>

#include "core/common/public/common.h"
#include "core/errors/public/errors.h"
#include "core/threads/public/threads.h"
#include "core/network/public/network.h"

#include "core/network/misc/tests.pb.h"
#include "core/network/misc/echoserver.h"

int main(int argc, char* argv[])
{
  if (argc < 4) {
    std::cout << "Usage: " << argv[0] << " <host> <port> <local/remote>" << std::endl;
    exit(1);
  }
  NetworkOptions options;
  options.set_host(argv[1]);
  options.set_port(atoi(argv[2]));
  options.set_max_packet_size(1024);
  if (strcmp(argv[3], "local") == 0) {
    options.set_socket_family(PF_UNIX);
    options.set_sin_path("/tmp/__echoserver__");
  } else {
    options.set_socket_family(PF_INET);
  }

  EventLoopImpl ss;
  EchoServer echo_server(&ss, options);
  if (echo_server.Start() != 0) {
    // What the hell happened
    std::cout << "Server failed to start\n";
    return 1;
  }
  ss.loop();
  return 0;
}
