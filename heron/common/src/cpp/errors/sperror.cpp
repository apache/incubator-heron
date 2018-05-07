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

#include "errors/sperror.h"
#include <string>
#include "errors/syserr.h"
#include "errors/sperrimpl.h"
#include "errors/sys-einfo-gen.h"
#include "errors/sys-einfo-bakw-gen.h"
#include "glog/logging.h"

namespace heron {
namespace error {

const char COLON = ':';

//
// Thread safety is not ensured yet. The idea is that the error modules will be
// loaded before the start of the program - will address this later
//

static Error_Store* g_error_store = NULL;

bool Error::initialize(void) {
  if (!g_error_store) {
    g_error_store = new Error_Store;
  }

  // Load the system module errors right away
  if (g_error_store) {
    load_module_errors("HERONSYS", sys_error_info, sys_error_info_bakw, SYS_ERRCNT);
  }

  return g_error_store ? true : false;
}

bool Error::shutdown(void) {
  if (g_error_store) {
    unload_module_errors_all();
    delete g_error_store;
    g_error_store = NULL;
  }

  return !g_error_store ? true : false;
}

bool Error::load_module_errors(const std::string& _modname, error_info_t* _errs,
                               error_info_t* _errnostrs, sp_int32 _errcnt) {
  if (g_error_store) {
    return g_error_store->load_module_errors(_modname, _errs, _errnostrs, _errcnt);
  }

  return false;
}

bool Error::unload_module_errors(const std::string& _modname) {
  if (g_error_store) {
    return g_error_store->unload_module_errors(_modname);
  }

  return false;
}

bool Error::unload_module_errors_all(void) {
  if (g_error_store) {
    return g_error_store->unload_module_errors_all();
  }

  return false;
}

std::string Error::get_error_msg(sp_uint32 _errno) {
  Error_Store* store = g_error_store;

  CHECK_NOTNULL(store);
  std::string msg = store->get_error_msg(_errno);

  if (msg.empty()) {
    LOG(WARNING) << "error message for " << _errno << " not found";
  }

  return msg;
}

std::string Error::get_errno_str(sp_uint32 _errno) {
  Error_Store* store = g_error_store;

  CHECK_NOTNULL(store);
  std::string msg = store->get_errno_str(_errno);

  if (msg.empty()) {
    LOG(WARNING) << "errno string for " << _errno << " not found";
  }

  return msg;
}

std::string Error::get_errno_msg(sp_uint32 _errno) {
  Error_Store* store = g_error_store;

  CHECK_NOTNULL(store);
  std::string msg = store->get_errno_str(_errno);

  if (msg.empty()) {
    LOG(WARNING) << "errno message for " << _errno << " not found";
    return msg;
  }

  msg += COLON;
  return (msg += store->get_error_msg(_errno));
}

std::string Error::get_module_errno_msg(sp_uint32 _errno) {
  Error_Store* store = g_error_store;

  CHECK_NOTNULL(store);

  std::string msg = store->get_module_name(_errno);
  if (msg.empty()) {
    LOG(WARNING) << "module name for " << _errno << " not found";
    return msg;
  }

  msg += COLON;
  msg += store->get_errno_str(_errno);

  msg += COLON;
  return (msg += store->get_error_msg(_errno));
}

std::string Error::get_module_error_msg(sp_uint32 _errno) {
  Error_Store* store = g_error_store;

  CHECK_NOTNULL(store);

  std::string msg = store->get_module_name(_errno);
  if (msg.empty()) {
    LOG(WARNING) << "module name for " << _errno << " not found";
    return msg;
  }

  msg += COLON;
  return (msg += store->get_error_msg(_errno));
}

std::string Error::get_error_module(sp_uint32 _errno) {
  Error_Store* store = g_error_store;

  CHECK_NOTNULL(store);
  std::string msg = store->get_module_name(_errno);

  if (msg.empty()) {
    LOG(WARNING) << "module name for " << _errno << " not found";
  }

  return msg;
}
}  // namespace error
}  // namespace heron
