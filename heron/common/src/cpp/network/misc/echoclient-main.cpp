#include <iostream>

#include "core/common/public/common.h"
#include "core/errors/public/errors.h"
#include "core/threads/public/threads.h"

#include "core/network/public/network.h"
#include "core/network/misc/tests.pb.h"
#include "core/network/misc/echoclient.h"

int main(int argc, char* argv[])
{
  if (argc < 4) {
    std::cout << "Usage: " << argv[0] << "<host> <port> <local/remote>" << std::endl;
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

  bool perf = false;
  if (argc > 4 && strcmp(argv[4], "perf") == 0)
    perf = true;
  EventLoopImpl ss;
  EchoClient echo_client(&ss, options, perf);
  echo_client.Start();
  ss.loop();
  return 0;
}
