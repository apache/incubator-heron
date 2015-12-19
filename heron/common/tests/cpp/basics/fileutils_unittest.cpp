#include "gtest/gtest.h"

#include "basics/basics.h"
#include "basics/fileutils.h"
#include "basics/modinit.h"

TEST(FileUtilsTest, basename)
{
  const std::string file1("/users/newuser/temp.file");
  EXPECT_EQ("temp.file", FileUtils::baseName(file1));

  const std::string file2("temp.file");
  EXPECT_EQ("temp.file", FileUtils::baseName(file2));

  const std::string file3("/users/newuser/tempfile");
  EXPECT_EQ("tempfile", FileUtils::baseName(file3));

  const std::string file4("tempfile");
  EXPECT_EQ("tempfile", FileUtils::baseName(file4));

  const std::string file5("newuser/tempfile");
  EXPECT_EQ("tempfile", FileUtils::baseName(file5));

  const std::string file6("");
  EXPECT_EQ("", FileUtils::baseName(file6));
}

int
main(int argc, char **argv)
{
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
