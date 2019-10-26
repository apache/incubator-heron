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

#include "errors/modinit.h"
#include "errors/gexception.h"
#include "errors/sperror.h"

static heron::error::Global_Exception_Handler* g_exception = NULL;

sp_int32 __nifty_error_modinit::count_ = 0;

__nifty_error_modinit::__nifty_error_modinit() {
  if (count_ == 0) {
    g_exception = new heron::error::Global_Exception_Handler;
    heron::error::Error::initialize();
    count_++;
  }
}

__nifty_error_modinit::~__nifty_error_modinit() {
  count_--;
  if (count_ == 0) {
    delete g_exception;
    heron::error::Error::shutdown();
  }
}
