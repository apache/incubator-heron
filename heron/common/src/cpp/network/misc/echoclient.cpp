#include "core/network/misc/echoclient.h"
#include <stdio.h>
#include <iostream>

EchoClient::EchoClient(EventLoopImpl* eventLoop, const NetworkOptions& _options,
                       bool _perf)
  : Client(eventLoop, _options), nrequests_(0), perf_(_perf)
{
  InstallResponseHandler(new EchoServerRequest(), &EchoClient::HandleEchoResponse);
}

EchoClient::~EchoClient()
{
}

void EchoClient::CreateAndSendRequest()
{
  char buf[1024];
  if (!perf_) {
    char* line = fgets(buf, 1024, stdin);
    if (line == NULL) {
      std::cout << "readline Error in EchoClient!! Bailing out!!\n";
      Stop();
      return;
    }
    if (line[strlen(line) - 1] == '\n') {
      line[strlen(line) - 1] = '\0';
    }
  } else {
    strcpy(buf, "I love you");
  }
  std::string tosend(buf);
  EchoServerRequest* request = new EchoServerRequest();
  request->set_echo_request(tosend);
  SendRequest(request, NULL);
  nrequests_++;
  return;
}

void EchoClient::HandleConnect(NetworkErrorCode _status)
{
  if (_status == OK) {
    std::cout << "Connected to " << get_clientoptions().get_host()
         << ":" << get_clientoptions().get_port() << std::endl;
    if (perf_) {
      for (int i = 0; i < 1000; ++i) {
        CreateAndSendRequest();
      }
    } else {
      CreateAndSendRequest();
    }
  } else {
    std::cout << "Could not connect to " << get_clientoptions().get_host()
         << ":" << get_clientoptions().get_port() << std::endl;
    Stop();
  }
}

void EchoClient::HandleClose(NetworkErrorCode)
{
  std::cout << "Server connection closed\n";
  getEventLoop()->loopExit();
}

void EchoClient::HandleEchoResponse(void*, EchoServerResponse* _response,
                                    NetworkErrorCode _status)
{
  if (_status != OK) {
    std::cout << "HandleEchoResponse got an error " << _status << "\n";
  } else {
    if (!perf_) {
      std::cout << _response->echo_response() << std::endl;
    } else {
      if (nrequests_ % 1000 == 0) {
        std::cout << "Received " << nrequests_ << " responses\n";
      }
    }
  }
  delete _response;
  CreateAndSendRequest();
}
