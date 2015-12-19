#include <sys/time.h>
#include "gtest/gtest.h"

#include "basics/basics.h"
#include "basics/modinit.h"

TEST(Time_Now_Test, test_now_time)
{
  struct timeval t;

  if (gettimeofday(&t, NULL) != 0)
  {
    ADD_FAILURE(); return ;
  }

  // Convert seconds and microseconds to seconds
  sp_double64 nowd = t.tv_sec * US_SECOND + t.tv_usec;
  EXPECT_NEAR(nowd, sp_time::now().usecs(), 10);
}

int
main(int argc, char **argv)
{
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
