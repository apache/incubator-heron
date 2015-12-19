#include "gtest/gtest.h"

#include "basics/basics.h"
#include "basics/modinit.h"

TEST(RIDTest, zero_rid)
{
  REQID        a;
  std::string  v(REQID_size, 0);
  EXPECT_EQ(a.str(), v);
}

TEST(RIDTest, itos_int)
{
  REQID            a, b;
  REQID_Generator  g;
  std::string      v(REQID_size, 0);

  EXPECT_EQ(REQID_size, a.length());
  EXPECT_EQ(a.str(), v);

  // check generation
  b = g.generate();
  EXPECT_EQ(REQID_size, b.length());
  EXPECT_NE(v, b.str());

  // check assignment
  a = b;
  EXPECT_STREQ(b.c_str(), a.c_str());

  // check equality
  ASSERT_TRUE(a == b);

  // check assign function
  std::string      d(REQID_size, 'a');
  a.assign(std::string(REQID_size, 'a'));
  EXPECT_EQ(d, a.str());

  // check non equality
  ASSERT_TRUE(a != b);
}

int
main(int argc, char **argv)
{
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
