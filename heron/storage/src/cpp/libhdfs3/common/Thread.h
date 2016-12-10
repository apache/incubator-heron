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
#ifndef _HDFS_LIBHDFS3_COMMON_THREAD_H_
#define _HDFS_LIBHDFS3_COMMON_THREAD_H_

#include <signal.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include "common/platform.h"

namespace Hdfs {
namespace Internal {

using std::thread;
using std::mutex;
using std::lock_guard;
using std::unique_lock;
using std::condition_variable;
using std::defer_lock_t;
using std::once_flag;
using std::call_once;

}  // namespace Internal
}  // namespace Hdfs

namespace Hdfs {
namespace Internal {

/*
 * make the background thread ignore these signals (which should allow that
 * they be delivered to the main thread)
 */
sigset_t ThreadBlockSignal();

/*
 * Restore previous signals.
 */
void ThreadUnBlockSignal(sigset_t sigs);

}  // namespace Internal
}  // namespace Hdfs

#define CREATE_THREAD(retval, fun) \
    do { \
        sigset_t sigs = Hdfs::Internal::ThreadBlockSignal(); \
        try { \
            retval = Hdfs::Internal::thread(fun); \
            Hdfs::Internal::ThreadUnBlockSignal(sigs); \
        } catch (...) { \
            Hdfs::Internal::ThreadUnBlockSignal(sigs); \
            throw; \
        } \
    } while (0)

#endif /* _HDFS_LIBHDFS3_COMMON_THREAD_H_ */
