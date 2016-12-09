/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef _HDFS_LIBHDFS3_EXCEPTION_EXCEPTIONINTERNAL_H_
#define _HDFS_LIBHDFS3_EXCEPTION_EXCEPTIONINTERNAL_H_

#include "platform.h"

#include <cassert>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <unistd.h>
#include <string>
#include <sstream>

#include "Function.h"
#include "StackPrinter.h"

#define STACK_DEPTH 64

#define PATH_SEPRATOR '/'
inline static const char * SkipPathPrefix(const char * path) {
    int i, len = strlen(path);

    for (i = len - 1; i > 0; --i) {
        if (path[i] == PATH_SEPRATOR) {
            break;
        }
    }

    assert(i > 0 && i < len);
    return path + i + 1;
}

#include <exception>
#include <stdexcept>

namespace Hdfs {
using std::rethrow_exception;
using std::current_exception;
using std::make_exception_ptr;
using std::exception_ptr;
}

namespace Hdfs {
using std::throw_with_nested;
using std::rethrow_if_nested;
}

namespace Hdfs {
namespace Internal {
template<typename THROWABLE>
ATTRIBUTE_NORETURN ATTRIBUTE_NOINLINE
void ThrowException(bool nested, const char * f, int l,
                    const char * exceptionName, const char * fmt, ...) __attribute__((format(printf, 5, 6)));

template<typename THROWABLE>
ATTRIBUTE_NORETURN ATTRIBUTE_NOINLINE
void ThrowException(bool nested, const char * f, int l,
                    const char * exceptionName, const char * fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    std::string buffer;
    buffer = exceptionName;
    buffer.append(": ");
    int size = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    int offset = buffer.size();
    buffer.resize(offset + size + 1);
    va_start(ap, fmt);
    vsnprintf(&buffer[offset], size + 1, fmt, ap);
    va_end(ap);

    if (!nested) {
        throw THROWABLE(buffer.c_str(), SkipPathPrefix(f), l,
                        Hdfs::Internal::PrintStack(1, STACK_DEPTH).c_str());
    } else {
        Hdfs::throw_with_nested(
            THROWABLE(buffer.c_str(), SkipPathPrefix(f), l,
                      Hdfs::Internal::PrintStack(1, STACK_DEPTH).c_str()));
    }

    throw std::logic_error("should not reach here.");
}

}  //  namespace Internal
}  //  namespace Hdfs

namespace Hdfs {

/**
 * A user defined callback function used to check if a slow operation has been canceled by the user.
 * If this function return true, HdfsCanceled will be thrown.
 */
extern function<bool(void)> ChecnOperationCanceledCallback;

class HdfsException;

}

namespace Hdfs {
namespace Internal {

/**
 * Check if a slow operation has been canceled by the user.
 * @throw return false if operation is not canceled, else throw HdfsCanceled.
 * @throw HdfsCanceled
 */
bool CheckOperationCanceled();

/**
 * Get a exception's detail message.
 * If the exception contains a nested exception, recursively get all the nested exception's detail message.
 * @param e The exception which detail message to be return.
 * @return The exception's detail message.
 */
const char *GetExceptionDetail(const Hdfs::HdfsException &e,
                               std::string &buffer);

/**
 * Get a exception's detail message.
 * If the exception contains a nested exception, recursively get all the nested exception's detail message.
 * @param e The exception which detail message to be return.
 * @return The exception's detail message.
 */
const char *GetExceptionDetail(const exception_ptr e, std::string &buffer);

const char * GetExceptionMessage(const exception_ptr e, std::string & buffer);

/**
 * Get a error information by the given system error number.
 * @param eno System error number.
 * @return The error information.
 * @throw nothrow
 */
const char * GetSystemErrorInfo(int eno);

}
}

#define THROW(throwable, fmt, ...) \
    Hdfs::Internal::ThrowException<throwable>(false, __FILE__, __LINE__, #throwable, fmt, ##__VA_ARGS__);

#define NESTED_THROW(throwable, fmt, ...) \
    Hdfs::Internal::ThrowException<throwable>(true, __FILE__, __LINE__, #throwable, fmt, ##__VA_ARGS__);

#endif /* _HDFS_LIBHDFS3_EXCEPTION_EXCEPTIONINTERNAL_H_ */
