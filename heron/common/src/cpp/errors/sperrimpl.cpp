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

#include "errors/sperrimpl.h"
#include <string.h>
#include <errno.h>
#include <iostream>
#include <string>
#include "glog/logging.h"

namespace heron {
namespace error {

Module_Errors::Module_Errors(const std::string& _modname, error_info_t* _errs,
                             error_info_t* _errstrs, sp_int32 _errcnt) {
  if (_errcnt <= 0) {
    return;
  }

  // Copy the error base and count of the number of errors
  errno_base_ = _errs[0].errnum_;
  errno_count_ = _errcnt;

  // Copy the module name
  mod_name_ = _modname;

  // Iterate through the error list and copy into the vector
  mod_errors_.clear();

  for (sp_int32 i = 0; i < _errcnt; i++) {
    mod_errors_.push_back(Error_Info(_errs[i].errnum_, std::string(_errs[i].errstr_),
                                     std::string(_errstrs[i].errstr_)));
  }
}

Module_Errors::~Module_Errors() { mod_errors_.clear(); }

bool Module_Errors::is_in_range(sp_uint32 _errno) {
  sp_uint32 start = errno_base_;
  sp_uint32 end = start + errno_count_;

  return (start <= _errno && _errno <= end) ? true : false;
}

const std::string& Module_Errors::get_error_msg(sp_uint32 _errno) {
  return mod_errors_[_errno - errno_base_].get_errstr();
}

const std::string& Module_Errors::get_errno_msg(sp_uint32 _errno) {
  return mod_errors_[_errno - errno_base_].get_errno_str();
}

bool Error_Store::load_module_errors(const std::string& _modname, error_info_t* _errs,
                                     error_info_t* _errnostrs, sp_int32 _errcnt) {
  // If the module exists, report error and exit
  if (moderrs_.find(_modname) != moderrs_.end()) {
    LOG(WARNING) << "Errors for module " << _modname << " already loaded";
    return false;
  }

  // Module does not exist...
  // Get the start and end of error range for the module
  sp_uint32 start = _errs[0].errnum_;
  sp_uint32 end = start + _errcnt;

  // Now check if there is an overlap of error space with other modules
  for (auto it = moderrs_.begin(); it != moderrs_.end(); it++) {
    // Get the start and end of error range for the current module
    sp_uint32 mstart = it->second->get_errno_base();
    sp_uint32 mend = mstart + it->second->get_errno_count();

    // Check if the start of the range falls in the current module error range
    if (start >= mstart && start <= mend) {
      return false;
    }

    // Check if the end of the range falls in the current module error range
    if (end >= mstart && end <= mend) {
      return false;
    }
  }

  // Everything is fine. Now create and insert the new error module
  moderrs_[_modname] = new Module_Errors(_modname, _errs, _errnostrs, _errcnt);

  return true;
}

bool Error_Store::unload_module_errors(const std::string& _modname) {
  // If the module exists, delete its errors
  auto it = moderrs_.find(_modname);
  if (it != moderrs_.end()) {
    delete it->second;
    moderrs_.erase(it);
    return true;
  }

  LOG(WARNING) << "Errors for module " << _modname << " does not exist";
  return false;
}

bool Error_Store::unload_module_errors_all(void) {
  while (!moderrs_.empty()) {
    Module_Errors::Map::iterator it = moderrs_.begin();
    delete it->second;
    moderrs_.erase(it);
  }

  return false;
}

std::string Error_Store::get_error_msg(sp_uint32 _errno) {
  std::string msg;

  for (auto it = moderrs_.begin(); it != moderrs_.end(); it++) {
    if (it->second->is_in_range(_errno)) {
      const std::string& errstr = it->second->get_error_msg(_errno);
      return (msg = errstr);
    }
  }

  // Errno is not found in any of our modules. Check for OS errors.
  msg = get_syserr_str(_errno);
  return msg.empty() ? std::string() : msg;
}

std::string Error_Store::get_module_name(sp_uint32 _errno) {
  Module_Errors::Map::iterator it;
  std::string msg;

  for (it = moderrs_.begin(); it != moderrs_.end(); it++) {
    if (it->second->is_in_range(_errno)) {
      const std::string& mname = it->second->get_module_name();
      return (msg = mname);
    }
  }

  // Errno is not found in any of our modules. Check for OS errors.
  msg = get_syserr_str(_errno);
  return msg.empty() ? std::string() : "OS";
}

std::string Error_Store::get_errno_str(sp_uint32 _errno) {
  std::string msg;

  for (auto it = moderrs_.begin(); it != moderrs_.end(); it++) {
    if (it->second->is_in_range(_errno)) {
      const std::string& errnostr = it->second->get_errno_msg(_errno);
      return (msg = errnostr);
    }
  }

  // Errno is not found in any of our modules. Check for OS errors.
  msg = get_syserr_str(_errno);
  return msg.empty() ? std::string() : "OS";
}

std::string Error_Store::get_syserr_str(sp_uint32 _errno) {
  char err_msg[BUFSIZ];
  std::string error_msg;

  error_msg = ::strerror_r(_errno, err_msg, sizeof(err_msg));
  return errno == EINVAL ? "" : error_msg;
}
}  // namespace error
}  // namespace heron
