#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "config/physical-plan-helper.h"

namespace heron { namespace config {

void PhysicalPlanHelper::GetLocalTasks(
              const proto::system::PhysicalPlan& _pplan,
              const sp_string& _stmgr,
              std::map<sp_string, TaskData>& _return)
{
  for (sp_int32 i = 0; i < _pplan.instances_size(); ++i) {
    const proto::system::Instance& instance = _pplan.instances(i);
    if (instance.stmgr_id() == _stmgr) {
      TaskData tdata;
      tdata.task_id_ = instance.info().task_id();
      tdata.component_name_ = instance.info().component_name();
      _return[instance.instance_id()] = tdata;
    }
  }
  return;
}

void PhysicalPlanHelper::GetLocalSpouts(
                const proto::system::PhysicalPlan& _pplan,
                const sp_string& _stmgr,
                std::set<sp_int32>& _return)
{
  std::set<sp_string> spouts;
  for (sp_int32 i = 0; i < _pplan.topology().spouts_size(); ++i) {
    spouts.insert(_pplan.topology().spouts(i).comp().name());
  }
  for (sp_int32 i = 0; i < _pplan.instances_size(); ++i) {
    const proto::system::Instance& instance = _pplan.instances(i);
    if (instance.stmgr_id() == _stmgr &&
        spouts.find(instance.info().component_name()) != spouts.end()) {
      _return.insert(instance.info().task_id());
    }
  }
  return;
}

void PhysicalPlanHelper::LogPhysicalPlan(
                const proto::system::PhysicalPlan& _pplan)
{
  LOG(INFO) << "Printing Physical Plan" << std::endl;
  LOG(INFO) << "Topology Name: " << _pplan.topology().name();
  LOG(INFO) << "Topology Id: " << _pplan.topology().id();
  LOG(INFO) << "Number of Stmgrs: " << _pplan.stmgrs_size();
  for (sp_int32 i = 0; i < _pplan.stmgrs_size(); ++i) {
    const proto::system::StMgr& stmgr = _pplan.stmgrs(i);
    LOG(INFO) << "\tStMgr id: " << stmgr.id();
    LOG(INFO) << "\tStMgr host_name: " << stmgr.host_name();
    LOG(INFO) << "\tStMgr data_port: " << stmgr.data_port();
    LOG(INFO) << "\tStMgr local_endpoint: " << stmgr.local_endpoint();
    LOG(INFO) << "\tStMgr cwd: " << stmgr.cwd();
    LOG(INFO) << "\tStMgr PID: " << stmgr.pid();
  }
  LOG(INFO) << "Total number of Instances: " << _pplan.instances_size();
  for (sp_int32 i = 0; i < _pplan.instances_size(); ++i) {
    const proto::system::Instance& instance = _pplan.instances(i);
    LOG(INFO) << "\tInstance id: " << instance.instance_id();
    LOG(INFO) << "\tMy Stmgr id: " << instance.stmgr_id();
    LOG(INFO) << "\tMy task_id: " << instance.info().task_id();
    LOG(INFO) << "\tMy component_index: " << instance.info().component_index();
    LOG(INFO) << "\tMy component name: " << instance.info().component_name();
  }

  LOG(INFO) << "Topology State: " << _pplan.topology().state();
}

}} // end namespace
