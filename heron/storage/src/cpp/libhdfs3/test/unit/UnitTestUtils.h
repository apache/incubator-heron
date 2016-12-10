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
#ifndef _HDFS_LIBHDFS3_TEST_UNIT_UNITTESTUTILS_H_
#define _HDFS_LIBHDFS3_TEST_UNIT_UNITTESTUTILS_H_

#include "DateTime.h"
#include "ExceptionInternal.h"

namespace Hdfs {
namespace Internal {

template<typename T>
void InvokeThrow(const char * str, bool * trigger) {
    if (!trigger || *trigger) {
        THROW(T, "%s", str);
    }
}

template<typename T, typename R>
R InvokeThrowAndReturn(const char * str, bool * trigger, R ret) {
    if (!trigger || *trigger) {
        THROW(T, "%s", str);
    }

    return ret;
}

template<typename R>
R InvokeWaitAndReturn(int timeout, R ret, int ec) {
    if (timeout > 0) {
        sleep_for(milliseconds(timeout));
    }

    errno = ec;
    return ret;
}

static inline void InvokeDoNothing() {

}

}
}
#endif /* _HDFS_LIBHDFS3_TEST_UNIT_UNITTESTUTILS_H_ */
