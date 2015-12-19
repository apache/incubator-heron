#include "gtest/gtest.h"

#include "basics/basics.h"
#include "basics/modinit.h"

TEST(UtilTest, itos_int)
{
  std::string value = std::to_string(1234567890);
  EXPECT_STREQ("1234567890", value.c_str());

  value = std::to_string(-1234567890);
  EXPECT_STREQ("-1234567890", value.c_str());
}

TEST(UtilTest, itos_uint)
{
  std::string value = std::to_string(1234567890);
  EXPECT_STREQ("1234567890", value.c_str());

  value = std::to_string(-1234567890);
  EXPECT_STREQ("-1234567890", value.c_str());
}

int
main(int argc, char **argv)
{
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
