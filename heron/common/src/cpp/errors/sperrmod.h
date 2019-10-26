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

#if !defined(__SP_ERRMOD_H)
#define __SP_ERRMOD_H

#include "basics/sptypes.h"

namespace heron {
namespace error {

/**
 * error_info_t is the structure used for generating the error headers from an
 * error description file using tools/errors.pl.
 */
struct error_info_t {
  sp_uint32 errnum_;    //! error number provided by the module author
  const char* errstr_;  //! error description by the module author
};

/**
 * error_impl_t is the per thread structure used for implementing errno and
 * errmsg
 */
struct errno_impl_t {
  sp_uint32 errnum_;    //! for storing the last occurred error
  const char* errstr_;  //! temporary storage for the error msg
};
}  // namespace error
}  // namespace heron

#endif /* end of header file */
