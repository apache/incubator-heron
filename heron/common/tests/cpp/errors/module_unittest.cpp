/*
 * Copyright 2015 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "gtest/gtest.h"

#include "basics/basics.h"
#include "errors/errors.h"

#include "errors/testerrs-error-enum-gen.h"
#include "errors/testerrs-einfo-gen.h"
#include "errors/testerrs-einfo-bakw-gen.h"

#include "basics/modinit.h"
#include "errors/modinit.h"

TEST(ModuleTest, module_load) {
  bool status = heron::error::Error::load_module_errors("test-errors", testerrs_error_info,
                                                        testerrs_error_info_bakw, TESTERRS_ERRCNT);
  EXPECT_TRUE(status);

  status = heron::error::Error::load_module_errors("test-errors", testerrs_error_info,
                                                   testerrs_error_info_bakw, TESTERRS_ERRCNT);
  EXPECT_FALSE(status);

  status = heron::error::Error::unload_module_errors("test-errors");
  EXPECT_TRUE(status);

  status = heron::error::Error::unload_module_errors("test-errors");
  EXPECT_FALSE(status);
}

int main(int argc, char **argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
