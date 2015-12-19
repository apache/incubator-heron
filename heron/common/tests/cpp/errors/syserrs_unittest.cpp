#include "gtest/gtest.h"

#include "basics/basics.h"
#include "errors/errors.h"

#include "basics/modinit.h"
#include "errors/modinit.h"

TEST(SysErrorsTest, heron_sys_errors)
{
}

TEST(SysErrorsTest, os_sys_errors)
{
}

int
main(int argc, char **argv)
{
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
