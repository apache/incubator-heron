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

#if !defined(__SP_ERRIMPL_H)
#define __SP_ERRIMPL_H

#include <map>
#include <string>
#include <vector>
#include "basics/sptypes.h"
#include "errors/sperrmod.h"

namespace heron {
namespace error {

/**
 * Error_Info class describes the information about an error. It contains
 * the error number and the corresponding error strings and provides
 * methods to access these members.
 */
class Error_Info {
 public:
  //! Constructor
  Error_Info() {}

  Error_Info(sp_uint32 _err, const std::string& _estr, const std::string& _enostr)
      : errno_(_err), errstr_(_estr), errnostr_(_enostr) {}

  //! Destructor
  ~Error_Info() {}

  //! Get the error number
  sp_uint32 get_errno() const { return errno_; }

  //! Get the error string in std::string format
  const std::string& get_errstr() const { return errstr_; }

  //! Get the error no string in std::string format
  const std::string& get_errno_str() const { return errnostr_; }

  //! Define a collection type for holding several errors
  typedef std::vector<Error_Info> Vector;

 protected:
  sp_uint32 errno_;       //! for storing the error number
  std::string errstr_;    //! for storing the error string
  std::string errnostr_;  //! for storing the error number string
};

/**
 * Module_Errors class contains information about all possible errors that can
 * occur in a module.
 */
class Module_Errors {
 public:
  //! Constructor
  Module_Errors() {}

  //! Constructor that loads the module errors during creation
  Module_Errors(const std::string& _modname, error_info_t* _errs, error_info_t* _errstrs,
                sp_int32 _errcnt);

  //! Destructor
  ~Module_Errors();

  //! Get the errno base for this module
  sp_uint32 get_errno_base() { return errno_base_; }

  //! Get the total number of errors for this module
  sp_int32 get_errno_count() { return errno_count_; }

  //! Get the module name
  const std::string& get_module_name() { return mod_name_; }

  //! Given an error no, check if it is in the module range
  bool is_in_range(sp_uint32 _errno);

  //! Given an error no, get the error message
  const std::string& get_error_msg(sp_uint32 _errno);

  //! Given an error no, get the errno message
  const std::string& get_errno_msg(sp_uint32 _errno);

  //! Define a map type for holding several modules and their errors
  typedef std::map<std::string, Module_Errors*> Map;

 protected:
  sp_uint32 errno_base_;  //! error base number for this module
  sp_int32 errno_count_;  //! number of errors for this module

  std::string mod_name_;           //! name of the module
  Error_Info::Vector mod_errors_;  //! actual errors of the module
};

//! Forward declaration for error_info_t that contains the generated errors and messages
struct error_info_t;

/*
 * Error_Store class is a singleton that holds the errors of several modules.
 * This class defines the global error space for a given process including
 * system errors.
 */
class Error_Store {
 public:
  //! Constructor and Destructors
  Error_Store() {}
  ~Error_Store() {}

  //! Load the errors of the given module into the global error space
  bool load_module_errors(const std::string& _modname, error_info_t* _errs,
                          error_info_t* _errnostrs, sp_int32 _errcnt);

  //! Unload the errors of the given module from the global error space
  bool unload_module_errors(const std::string& _modname);

  //! Unload all the error modules
  bool unload_module_errors_all(void);

  //! Get the error msg for an errno
  std::string get_error_msg(sp_uint32 _errno);

  //! Get the module name for an errno
  std::string get_module_name(sp_uint32 _errno);

  //! Get the error no string
  std::string get_errno_str(sp_uint32 _errno);

 protected:
  //! Get OS system error string
  std::string get_syserr_str(sp_uint32 _errno);

  Module_Errors::Map moderrs_;  //! the global error space for a process
};
}  // namespace error
}  // namespace heron

#endif /* end of header file */
