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
#include "grouping/lowest-grouping.h"

// Test to make sure that all task ids are returned
TEST(LowestGrouping, test_allgrouping)
{
  std::vector<sp_int32> task_ids;
  for (sp_int32 i = 0; i < 100; ++i) {
    task_ids.push_back(i);
  }

  heron::stmgr::LowestGrouping* g = new heron::stmgr::LowestGrouping(task_ids);
  for (sp_int32 i = 0; i < 1000; ++i) {
    heron::proto::system::HeronDataTuple dummy;
    std::list<sp_int32> dest;
    g->GetListToSend(dummy, dest);
    EXPECT_EQ(dest.size(), (sp_uint32)1);
    EXPECT_EQ(dest.front(), 0);
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
