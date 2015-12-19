#include "gtest/gtest.h"

#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "basics/modinit.h"
#include "errors/modinit.h"
#include "threads/modinit.h"
#include "network/modinit.h"

#include "grouping/grouping.h"
#include "grouping/custom-grouping.h"

// Custom grouping is done at the instance level.
// The stmgr has no role here. The stmgr should not
// chose any tasks
TEST(CustomGrouping, test_nullop)
{
  std::vector<sp_int32> task_ids;
  task_ids.push_back(0);
  task_ids.push_back(2);
  task_ids.push_back(4);
  task_ids.push_back(8);

  heron::stmgr::CustomGrouping* g = new heron::stmgr::CustomGrouping(task_ids);
  heron::proto::system::HeronDataTuple tuple;
  tuple.add_values("dummy");

  std::set<sp_int32> all_dests;
  for (sp_int32 i = 0; i < 1000; ++i) {
    std::list<sp_int32> dests;
    g->GetListToSend(tuple, dests);
    EXPECT_EQ(dests.size(), (sp_uint32)0);
  }

  delete g;
}

int
main(int argc, char **argv)
{
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
