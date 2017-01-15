#include <iostream>

#include "core/common/public/common.h"
#include "core/errors/public/errors.h"
#include "core/threads/public/threads.h"
#include "core/network/public/network.h"
#include "core/network/misc/samplehttpserver.h"

int main(int argc, char* argv[])
{
  Init("SampleHTTPServer", argc, argv);

  if (argc < 2) {
    cout << "Usage " << argv[0] << " <port>\n";
    exit(1);
  }

  EventLoopImpl ss;
  ServerOptions options;
  options.set_host("127.0.0.1");
  options.set_port(atoi(argv[1]));
  options.set_max_packet_size(0);
  SampleHTTPServer http_server(&ss, &options);
  ss.loop();
  return 0;
}
