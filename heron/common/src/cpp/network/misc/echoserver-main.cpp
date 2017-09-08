#include <iostream>

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "proto/messages.h"
#include "network/misc/echoserver.h"

int main(int argc, char* argv[])
{
  if (argc < 3) {
    std::cout << "Usage: " << argv[0] << " <host> <port>" << std::endl;
    exit(1);
  }
  NetworkOptions options;
  options.set_host(argv[1]);
  options.set_port(atoi(argv[2]));
  options.set_max_packet_size(1024);
  options.set_socket_family(PF_INET);

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
