#include "gtest/gtest.h"

#include "basics/basics.h"
#include "errors/errors.h"

#include "errors/testerrs-error-enum-gen.h"
#include "errors/testerrs-einfo-gen.h"
#include "errors/testerrs-einfo-bakw-gen.h"

#include "basics/modinit.h"
#include "errors/modinit.h"

TEST(ModuleTest, module_load)
{
  bool status = heron::error::Error::load_module_errors(
      "test-errors", testerrs_error_info,
      testerrs_error_info_bakw, TESTERRS_ERRCNT
  );
  EXPECT_TRUE(status);

  status = heron::error::Error::load_module_errors(
      "test-errors", testerrs_error_info,
      testerrs_error_info_bakw, TESTERRS_ERRCNT
  );
  EXPECT_FALSE(status);

  status = heron::error::Error::unload_module_errors("test-errors");
  EXPECT_TRUE(status);

  status = heron::error::Error::unload_module_errors("test-errors");
  EXPECT_FALSE(status);
}

int
main(int argc, char **argv)
{
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
