/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "gtest/gtest.h"

#include "Exception.h"
#include "ExceptionInternal.h"

#include <iostream>

using namespace Hdfs;
using namespace Hdfs::Internal;

TEST(TestException, ThrowAndCatch) {
    try {
        THROW(HdfsException, "hello world %d %lf", 123, 123.123);
    } catch (const HdfsException & e) {
        return;
    }

    ASSERT_TRUE(false);
}

TEST(TestException, ThrowAndCatchInheritedException) {
    try {
        THROW(HdfsException, "hello world %d %lf", 123, 123.123);
    } catch (const HdfsException & e) {
        return;
    }

    ASSERT_TRUE(false);
}

TEST(TestException, ThrowAndRethrow) {
    try {
        try {
            THROW(HdfsException, "hello world %d %lf", 123, 123.123);
        } catch (const HdfsException & e) {
            exception_ptr p = current_exception();
            rethrow_exception(p);
        }

        ASSERT_TRUE(false);
    } catch (const HdfsException & e) {
        return;
    }

    ASSERT_TRUE(false);
}

TEST(TestException, ThrowNestedException) {
    try {
        try {
            try {
                THROW(HdfsNetworkException, "hello world %d %lf", 123, 123.123);
            } catch (const HdfsException & e) {
                NESTED_THROW(HdfsNetworkException, "nested throw: ");
            }

            ASSERT_TRUE(false);
        } catch (const HdfsException & e) {
            Hdfs::rethrow_if_nested(e);
        }

        ASSERT_TRUE(false);
    } catch (const HdfsException & e) {
        std::cerr << e.what() << std::endl;
        std::cerr << e.msg() << std::endl;
        return;
    }

    ASSERT_TRUE(false);
}

TEST(TestException, NestedErrorDetails) {
    try {
        try {
            try {
                try {
                    try {
                        throw std::runtime_error("runtime_error");
                    } catch (...) {
                        NESTED_THROW(HdfsException, "nested HdfsException");
                    }
                } catch (...) {
                    Hdfs::throw_with_nested(std::runtime_error("runtime_error"));
                }
            } catch (...) {
                Hdfs::throw_with_nested(std::runtime_error("runtime_error"));
            }
        } catch (...) {
            NESTED_THROW(HdfsException, "nested HdfsException");
        }
    } catch (const HdfsException & e) {
        std::string buffer;
        std::cout << GetExceptionDetail(e, buffer) << std::endl;
    }
}
