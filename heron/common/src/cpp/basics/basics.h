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

////////////////////////////////////////////////////////////////////////////////
//
// Public basics module include file for use in other modules
//
///////////////////////////////////////////////////////////////////////////////

#if !defined(HERON_BASICS_H_)
#define HERON_BASICS_H_

#include "glog/logging.h"
#include "basics/sptypes.h"
#include "basics/sprcodes.h"
#include "basics/callback.h"
#include "basics/classcallback.h"
#include "basics/callback1.h"
#include "basics/classcallback1.h"
#include "basics/fileutils.h"
#include "basics/processutils.h"
#include "basics/iputils.h"
#include "basics/strutils.h"
#include "basics/randutils.h"
#include "basics/sockutils.h"
#include "basics/spconsts.h"
#include "basics/ridgen.h"
#include "basics/sphash.h"
#include "basics/spfuncs.h"
#include "basics/sptest.h"
#include "basics/mempool.h"

// The standard std::make_unique(...) was introduced starting from C++ 14. Heron uses C++ 11.
// Unfortunatelly we can not bump a compiler version used in Heron since we are tight coupled with
// CentOS 7 whih comes with very old version of GCC (4.8.5). That's why we introduced
// make_unique(...) manually here.
template<typename T, typename... Args>
std::unique_ptr<T> make_unique(Args&&... args) {
    return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}

#endif  // HERON_BASICS_H_
