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

#include "errors/gexception.h"
#include <stdlib.h>
#include <iostream>
#include "errors/spexcept.h"

namespace heron {
namespace error {

Global_Exception_Handler::Global_Exception_Handler() {
  static Singleton_Exception_Handler a_handler;
}

Global_Exception_Handler::Singleton_Exception_Handler::Singleton_Exception_Handler() {
  std::set_terminate(terminate);
}

void Global_Exception_Handler::Singleton_Exception_Handler::terminate() {
  try {
    throw;
  }

  catch (const Exception_Backtrace& e) {
    e.print_trace(std::cerr);
    std::cerr << "Known exception caught in global handler" << std::endl;
  }

  catch (...) {
    std::cerr << "Unknown exception caught in global handler" << std::endl;
  }

  abort();
}
}  // namespace error
}  // namespace heron
