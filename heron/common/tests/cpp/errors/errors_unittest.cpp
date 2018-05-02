/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <string>
#include "gtest/gtest.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "errors/testerrs-error-enum-gen.h"
#include "errors/testerrs-einfo-gen.h"
#include "errors/testerrs-einfo-bakw-gen.h"
#include "basics/modinit.h"
#include "errors/modinit.h"

TEST(ErrorsTest, error_msg) {
  heron::error::Error::load_module_errors("test-errors", testerrs_error_info,
                                          testerrs_error_info_bakw, TESTERRS_ERRCNT);

  EXPECT_EQ("It is the first error!", heron::error::Error::get_error_msg(TESTERRS_TEST_ERROR1));

  EXPECT_EQ("It is the second error!", heron::error::Error::get_error_msg(TESTERRS_TEST_ERROR2));

  EXPECT_EQ("It is the third error!", heron::error::Error::get_error_msg(TESTERRS_TEST_ERROR3));
}

TEST(ErrorsTest, errno_str) {
  heron::error::Error::load_module_errors("test-errors", testerrs_error_info,
                                          testerrs_error_info_bakw, TESTERRS_ERRCNT);

  EXPECT_EQ("TESTERRS_TEST_ERROR1", heron::error::Error::get_errno_str(TESTERRS_TEST_ERROR1));

  EXPECT_EQ("TESTERRS_TEST_ERROR2", heron::error::Error::get_errno_str(TESTERRS_TEST_ERROR2));

  EXPECT_EQ("TESTERRS_TEST_ERROR3", heron::error::Error::get_errno_str(TESTERRS_TEST_ERROR3));
}

TEST(ErrorsTest, error_module) {
  heron::error::Error::load_module_errors("test-errors", testerrs_error_info,
                                          testerrs_error_info_bakw, TESTERRS_ERRCNT);

  EXPECT_EQ("test-errors", heron::error::Error::get_error_module(TESTERRS_TEST_ERROR1));

  EXPECT_EQ("test-errors", heron::error::Error::get_error_module(TESTERRS_TEST_ERROR2));

  EXPECT_EQ("test-errors", heron::error::Error::get_error_module(TESTERRS_TEST_ERROR3));
}

TEST(ErrorsTest, errno_msg) {
  heron::error::Error::load_module_errors("test-errors", testerrs_error_info,
                                          testerrs_error_info_bakw, TESTERRS_ERRCNT);

  std::string expected = heron::error::Error::get_errno_str(TESTERRS_TEST_ERROR1) + ":" +
                         heron::error::Error::get_error_msg(TESTERRS_TEST_ERROR1);
  EXPECT_EQ(expected, heron::error::Error::get_errno_msg(TESTERRS_TEST_ERROR1));

  expected = heron::error::Error::get_errno_str(TESTERRS_TEST_ERROR2) + ":" +
             heron::error::Error::get_error_msg(TESTERRS_TEST_ERROR2);
  EXPECT_EQ(expected, heron::error::Error::get_errno_msg(TESTERRS_TEST_ERROR2));

  expected = heron::error::Error::get_errno_str(TESTERRS_TEST_ERROR3) + ":" +
             heron::error::Error::get_error_msg(TESTERRS_TEST_ERROR3);
  EXPECT_EQ(expected, heron::error::Error::get_errno_msg(TESTERRS_TEST_ERROR3));
}

TEST(ErrorsTest, module_errno_msg) {
  heron::error::Error::load_module_errors("test-errors", testerrs_error_info,
                                          testerrs_error_info_bakw, TESTERRS_ERRCNT);

  std::string expected = heron::error::Error::get_error_module(TESTERRS_TEST_ERROR1) + ":" +
                         heron::error::Error::get_errno_str(TESTERRS_TEST_ERROR1) + ":" +
                         heron::error::Error::get_error_msg(TESTERRS_TEST_ERROR1);
  EXPECT_EQ(expected, heron::error::Error::get_module_errno_msg(TESTERRS_TEST_ERROR1));

  expected = heron::error::Error::get_error_module(TESTERRS_TEST_ERROR2) + ":" +
             heron::error::Error::get_errno_str(TESTERRS_TEST_ERROR2) + ":" +
             heron::error::Error::get_error_msg(TESTERRS_TEST_ERROR2);
  EXPECT_EQ(expected, heron::error::Error::get_module_errno_msg(TESTERRS_TEST_ERROR2));

  expected = heron::error::Error::get_error_module(TESTERRS_TEST_ERROR3) + ":" +
             heron::error::Error::get_errno_str(TESTERRS_TEST_ERROR3) + ":" +
             heron::error::Error::get_error_msg(TESTERRS_TEST_ERROR3);
  EXPECT_EQ(expected, heron::error::Error::get_module_errno_msg(TESTERRS_TEST_ERROR3));
}

TEST(ErrorsTest, module_error_msg) {
  heron::error::Error::load_module_errors("test-errors", testerrs_error_info,
                                          testerrs_error_info_bakw, TESTERRS_ERRCNT);

  std::string expected = heron::error::Error::get_error_module(TESTERRS_TEST_ERROR1) + ":" +
                         heron::error::Error::get_error_msg(TESTERRS_TEST_ERROR1);
  EXPECT_EQ(expected, heron::error::Error::get_module_error_msg(TESTERRS_TEST_ERROR1));

  expected = heron::error::Error::get_error_module(TESTERRS_TEST_ERROR2) + ":" +
             heron::error::Error::get_error_msg(TESTERRS_TEST_ERROR2);
  EXPECT_EQ(expected, heron::error::Error::get_module_error_msg(TESTERRS_TEST_ERROR2));

  expected = heron::error::Error::get_error_module(TESTERRS_TEST_ERROR3) + ":" +
             heron::error::Error::get_error_msg(TESTERRS_TEST_ERROR3);
  EXPECT_EQ(expected, heron::error::Error::get_module_error_msg(TESTERRS_TEST_ERROR3));
}

int main(int argc, char **argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
