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

#if !defined(__SP_EXCEPTION_H)
#define __SP_EXCEPTION_H

#include <string>
#include "basics/sptypes.h"
#include "errors/sperror.h"

namespace heron {
namespace error {

/**
 * Abstract base class for any type of exception in Stream processing
 */
class Exception : public std::exception {
 public:
  Exception() {}

  virtual ~Exception() throw() {}
};

/**
 * Abstract base class for an exception that should kill the application.
 *
 * Threads often implement their own exception handlers, which will catch
 * an exception, log it, then continue on.  Sometimes an exception is too
 * severe for that behavior. In that case, an exception derived from this
 * class should be thrown.
 *
 * Any exception handlers for threads should catch and re-throw this exception.
 *
 */
class Fatal_Exception : public Exception {
 public:
  Fatal_Exception() {}

  virtual ~Fatal_Exception() throw() {}
};

/**
 * Abstract base class for any type of exception that can print a stack
 * backtrace. Stack trace could be implemented either
 *
 * a) using a combination of try/catch macros around function invocation
 * b) using C debug library using backtrace() and backtrace_symbols call
 *
 */
class Exception_Backtrace : public Exception {
 public:
  Exception_Backtrace() {}

  virtual ~Exception_Backtrace() throw() {}

  //! Print the trace of this exception to an ostream
  virtual void print_trace(std::ostream& to) const = 0;
};

/**
 * Exception class which automatically generates a backtrace, using the C
 * library's debugging facilities.
 *
 * The backtrace will be generated to the spot where the exception was thrown.
 */
class Exception_Auto_Backtrace : public Exception_Backtrace {
 public:
  Exception_Auto_Backtrace();

  virtual ~Exception_Auto_Backtrace() throw() {}

  //! Print the trace of this exception to an ostream
  virtual void print_trace(std::ostream& to) const;

 private:
  static const int MAX_BT_FRAMES_ = 200;  //! Max number of stack frames
};

/**
 * Encapsulates any system wide error into an exception. This is useful
 * for handling errors up in the function call chain.
 */
class Error_Exception : public Exception_Auto_Backtrace {
 public:
  //! Create a new error exception with the given error
  explicit Error_Exception(const sp_uint32 _errno) {
    errno_ = _errno;
    errstr_ = Error::get_error_msg(errno_);
  }

  virtual ~Error_Exception() throw() {}

  //! Get the error message corresponding to the error
  virtual const char* what() const throw();

  //! Get the errno for this exception
  sp_uint32 get_errno() const { return errno_; }

 private:
  sp_uint32 errno_;
  std::string errstr_;
};
}  // namespace error
}  // namespace heron

#endif /* end of header file */
