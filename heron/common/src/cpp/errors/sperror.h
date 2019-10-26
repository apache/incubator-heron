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

#if !defined(__SP_ERROR_H)
#define __SP_ERROR_H

#include <string>
#include "basics/sptypes.h"

/**
 * Error class is the gateway to access the global error address space.
 * It provides two different types of methods
 *
 *   - Methods for accessing the last error encountered and the error
 *     message in various formats.
 *
 *   - Methods for accessing the error messages in various formats given
 *     an errno in various formats.
 *
 *   - Methods for loading and unloading module errors into the global error
 *     address space.
 */

/*
 * NOTE:
 * These functions are thread safe. The char* pointer returned
 * for error messages from these functions use thread specific storage and
 * hence they are safe to use within a thread. However, passing the char*
 * pointer across MULTIPLE THREADS WILL NOT WORK. You need to explicitly copy
 * the message into seperately allocated memory and pass it.
 *
 */

namespace heron {
namespace error {

struct error_info_t;

class Error {
 public:
  //! Initialize the error module
  static bool initialize(void);

  //! Shutdown the error module
  static bool shutdown(void);

  //! Load the errors of the given module into the global error space
  static bool load_module_errors(const std::string& _modname, error_info_t* _errs,
                                 error_info_t* _errnostrs, sp_int32 _errcnt);

  //! Unload the errors of the given module from the global error space
  static bool unload_module_errors(const std::string& _modname);

  //! Unload the errors of all moduleis from the global error space
  static bool unload_module_errors_all(void);

  //! Get the error message for the given error no
  static std::string get_error_msg(sp_uint32 _errno);

  //! Get the string version of errno
  static std::string get_errno_str(sp_uint32 _errno);

  //! Get the module name for the given error code
  static std::string get_error_module(sp_uint32 _errno);

  //! Get the error message for the error code in the format <errorno:errormsg>
  static std::string get_errno_msg(sp_uint32 _errno);

  //! Get the error message for error code in format <modname:errorno:errormsg>
  static std::string get_module_errno_msg(sp_uint32 _errno);

  //! Get the error message for the error code in the format <modname:errormsg>
  static std::string get_module_error_msg(sp_uint32 _errno);
};
}  // namespace error
}  // namespace heron

#endif /* end of header file */
