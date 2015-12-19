#ifndef __TMASTER_STATS_INTERFACE_H_
#define __TMASTER_STATS_INTERFACE_H_

class HTTPServer;
class TMaster;
class EventLoop;
class NetworkOptions;
class IncomingHTTPRequest;

namespace heron { namespace tmaster {

class TMetricsCollector;

class StatsInterface
{
 public:
  StatsInterface(EventLoop* eventLoop, const NetworkOptions& options,
                 TMetricsCollector* _collector);
  virtual ~StatsInterface();

 private:
  void HandleStatsRequest(IncomingHTTPRequest* _request);
  void HandleUnknownRequest(IncomingHTTPRequest* _request);
  void HandleExceptionRequest(IncomingHTTPRequest* _request);
  void HandleExceptionSummaryRequest(IncomingHTTPRequest* _request);

  HTTPServer* http_server_;      // Our http server
  TMetricsCollector* metrics_collector_;
};

}} // end namespace

#endif
