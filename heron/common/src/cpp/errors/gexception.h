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

#if !defined(__GEXCEPTION_H)
#define __GEXCEPTION_H

/**
 * Singleton object which installs a global exception handler.
 *
 * When an object of this type is created globally, it will install itself
 * as the default exception handler, and will print out the exception then
 * call abort() if it is called.  It knows how to print out stack traces
 * for Lx_ErrorException.  If it is the first object instantiated, it will
 * also catch any exceptions thrown during construction of global objects.
 *
 * To use, put these two lines very first in the C++ file containing main():
 *
 *    #include "errhandler/gexception.h"
 *    Global_Exception_Handler       globalExceptionHandler;
 *
 * This object is a singleton, so creating more than one will not be harmful.
 *
 * This is based on an idea from IBM:
 *    http://www-128.ibm.com/developerworks/linux/library/l-cppexcep.html
 *
 */

namespace heron {
namespace error {

class Global_Exception_Handler {
 public:
  //! Create a singleton global exception handler to handle termination.
  Global_Exception_Handler();

  ~Global_Exception_Handler() {}

  //! Print out information about the current exception. Should be used in a catch block.
  static void print_backtrace();

 private:
  class Singleton_Exception_Handler {
   public:
    Singleton_Exception_Handler();

    ~Singleton_Exception_Handler() {}

    static void terminate(void);
  };
};
}  // namespace error
}  // namespace heron

#endif /* end of header file */
