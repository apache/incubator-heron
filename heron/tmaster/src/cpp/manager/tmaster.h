#ifndef __TMASTER_H
#define __TMASTER_H

#include <map>
#include <set>
#include <string>
#include <vector>

#include "metrics/metrics-mgr-st.h"
#include "metrics/metrics.h"


namespace heron { namespace proto {
  namespace system {
    class PhysicalPlan;
    class StMgr;
  }
  namespace api {
    class Topology;
  }
}}

namespace heron { namespace common {
  class HeronStateMgr;
}}

namespace heron { namespace tmaster {

class StMgrState;
class TController;
class StatsInterface;
class TMasterServer;
class TMetricsCollector;

class TMaster
{
 public:
  TMaster(const std::string& _zk_hostport, const std::string& _topology_name,
          const std::string& _topology_id,
          const std::string& _topdir, const std::vector<std::string>& _stmgrs,
          sp_int32 _controller_port, sp_int32 _master_port, sp_int32 _stats_port,
          sp_int32 metricsMgrPort, const std::string& metrics_sinks_yaml,
          const std::string& _myhost_name, EventLoop* eventLoop);

  virtual ~TMaster();

  const std::string& GetTopologyId() const { return topology_->id(); }
  const std::string& GetTopologyName() const { return topology_->name(); }
  proto::api::TopologyState GetTopologyState() const { return topology_->state(); }
  void ActivateTopology(VCallback<proto::system::StatusCode> cb);
  void DeActivateTopology(VCallback<proto::system::StatusCode> cb);
  proto::system::Status* RegisterStMgr(const proto::system::StMgr& _stmgr,
                                       const std::vector<proto::system::Instance*>& _instances,
                                       Connection* _conn,
                                       proto::system::PhysicalPlan*& _pplan);
  // function to update heartbeat for a nodemgr
  proto::system::Status* UpdateStMgrHeartbeat(Connection *_conn,
                              sp_int64 _time,
                              proto::system::StMgrStats* _stats);

  // When stmgr disconnects from us
  proto::system::StatusCode RemoveStMgrConnection(Connection* _conn);

  // Accessors
  const proto::api::Topology* getTopology() const { return topology_; }
  const proto::system::PhysicalPlan* getPhysicalPlan() const { return current_pplan_; }

 private:
  // Function to be called that calls MakePhysicalPlan and sends it to all stmgrs
  void DoPhysicalPlan(EventLoop::Status _code);

  // Big brother function that does the assignment to the workers
  // If _new_stmgr is null, this means that there was a plan
  // existing, but a _new_stmgr joined us. So redo his part
  // If _new_stmgr is empty, this means do pplan from scratch
  proto::system::PhysicalPlan* MakePhysicalPlan();

  // Check to see if the topology is of correct format
  bool ValidateTopology();

  // Check to see if the topology and stmgrs match
  // in terms of workers
  bool ValidateStMgrsWithTopology();

  // Check to see if the stmgrs and pplan match
  // in terms of workers
  bool ValidateStMgrsWithPhysicalPlan();

  // Function called after we set the tmasterlocation
  void SetTMasterLocationDone(proto::system::StatusCode _code);
  // Function called after we get the topology
  void GetTopologyDone(proto::system::StatusCode _code);

  // Function called after we try to get assignment
  void GetPhysicalPlanDone(proto::system::PhysicalPlan* _pplan,
                           proto::system::StatusCode _code);

  // Function called after we try to commit a new assignment
  void SetPhysicalPlanDone(proto::system::PhysicalPlan* _pplan,
                           proto::system::StatusCode _code);

  // Function called when we write topology
  void ActivateTopologyDone(VCallback<proto::system::StatusCode> cb,
                            proto::system::StatusCode _code);
  // Function called when we write topology
  void DeActivateTopologyDone(VCallback<proto::system::StatusCode> cb,
                              proto::system::StatusCode _code);

  // Function called when we want to setup ourselves as tmaster
  void EstablishTMaster(EventLoop::Status);

  void UpdateProcessMetrics(EventLoop::Status);

  // map of active stmgr id to stmgr state
  typedef std::map<std::string, StMgrState*>    StMgrMap;
  typedef StMgrMap::iterator                    StMgrMapIter;
  StMgrMap                                      stmgrs_;

  // map of connection to stmgr id
  std::map<Connection*, sp_string>              connection_to_stmgr_id_;

  // set of nodemanagers that have not yet connected to us
  std::set<std::string>                         absent_stmgrs_;

  // The current physical plan
  proto::system::PhysicalPlan*                  current_pplan_;

  // The topology as submitted by the user
  proto::api::Topology*                         topology_;

  // The statemgr where we store/retrieve our state
  heron::common::HeronStateMgr*                 state_mgr_;

  // Our copy of the tmasterlocation
  proto::tmaster::TMasterLocation*              tmaster_location_;

  // When we are in the middle of doing assignment
  // we set this to true
  bool                                          assignment_in_progress_;
  bool                                          do_reassign_;

  // State information
  std::string                                   zk_hostport_;
  std::string                                   topdir_;

  // Servers that implement our services
  TController*                                  controller_;
  sp_int32                                      controller_port_;
  TMasterServer*                                master_;
  sp_int32                                      master_port_;
  StatsInterface*                               stats_;
  sp_int32                                      stats_port_;
  std::string                                   myhost_name_;

  // how many times have we tried to establish
  // ourselves as master
  sp_int32                                      master_establish_attempts_;

  // collector
  TMetricsCollector*                        metrics_collector_;

  sp_int32 mMetricsMgrPort;
  // Metrics Manager
  heron::common::MetricsMgrSt* mMetricsMgrClient;

  // Process related metrics
  heron::common::MultiAssignableMetric* tmasterProcessMetrics;

  // The time at which the stmgr was started up
  std::chrono::high_resolution_clock::time_point start_time_;

  // Copy of the EventLoop
  EventLoop*                         eventLoop_;
};

}} // end namespace

#endif
