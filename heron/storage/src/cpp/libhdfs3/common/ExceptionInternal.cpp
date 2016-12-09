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
#include "platform.h"

#include "Exception.h"
#include "ExceptionInternal.h"
#include "Thread.h"

#include <cstring>
#include <cassert>
#include <sstream>

namespace Hdfs {

function<bool(void)> ChecnOperationCanceledCallback;

namespace Internal {

bool CheckOperationCanceled() {
    if (ChecnOperationCanceledCallback && ChecnOperationCanceledCallback()) {
        THROW(HdfsCanceled, "Operation has been canceled by the user.");
    }

    return false;
}

const char * GetSystemErrorInfo(int eno) {
    static THREAD_LOCAL char message[64];
    char buffer[64], *pbuffer;
    pbuffer = buffer;
    strerror_r(eno, buffer, sizeof(buffer));
    snprintf(message, sizeof(message), "(errno: %d) %s", eno, pbuffer);
    return message;
}

static void GetExceptionDetailInternal(const Hdfs::HdfsException & e,
                                       std::stringstream & ss, bool topLevel);

static void GetExceptionDetailInternal(const std::exception & e,
                                       std::stringstream & ss, bool topLevel) {
    try {
        if (!topLevel) {
            ss << "Caused by\n";
        }

        ss << e.what();
    } catch (const std::bad_alloc & e) {
        return;
    }

    try {
        Hdfs::rethrow_if_nested(e);
    } catch (const Hdfs::HdfsException & nested) {
        GetExceptionDetailInternal(nested, ss, false);
    } catch (const std::exception & nested) {
        GetExceptionDetailInternal(nested, ss, false);
    }
}

static void GetExceptionDetailInternal(const Hdfs::HdfsException & e,
                                       std::stringstream & ss, bool topLevel) {
    try {
        if (!topLevel) {
            ss << "Caused by\n";
        }

        ss << e.msg();
    } catch (const std::bad_alloc & e) {
        return;
    }

    try {
        Hdfs::rethrow_if_nested(e);
    } catch (const Hdfs::HdfsException & nested) {
        GetExceptionDetailInternal(nested, ss, false);
    } catch (const std::exception & nested) {
        GetExceptionDetailInternal(nested, ss, false);
    }
}

const char* GetExceptionDetail(const Hdfs::HdfsException& e,
                               std::string& buffer) {
    try {
        std::stringstream ss;
        ss.imbue(std::locale::classic());
        GetExceptionDetailInternal(e, ss, true);
        buffer = ss.str();
    } catch (const std::bad_alloc& e) {
        return "Out of memory";
    }

    return buffer.c_str();
}

const char* GetExceptionDetail(const exception_ptr e, std::string& buffer) {
    std::stringstream ss;
    ss.imbue(std::locale::classic());

    try {
        Hdfs::rethrow_exception(e);
    } catch (const Hdfs::HdfsException& nested) {
        GetExceptionDetailInternal(nested, ss, true);
    } catch (const std::exception& nested) {
        GetExceptionDetailInternal(nested, ss, true);
    }

    try {
        buffer = ss.str();
    } catch (const std::bad_alloc& e) {
        return "Out of memory";
    }

    return buffer.c_str();
}

static void GetExceptionMessage(const std::exception & e,
                                std::stringstream & ss, int recursive) {
    try {
        for (int i = 0; i < recursive; ++i) {
            ss << '\t';
        }

        if (recursive > 0) {
            ss << "Caused by: ";
        }

        ss << e.what();
    } catch (const std::bad_alloc & e) {
        return;
    }

    try {
        Hdfs::rethrow_if_nested(e);
    } catch (const std::exception & nested) {
        GetExceptionMessage(nested, ss, recursive + 1);
    }
}

const char * GetExceptionMessage(const exception_ptr e, std::string & buffer) {
    std::stringstream ss;
    ss.imbue(std::locale::classic());

    try {
        Hdfs::rethrow_exception(e);
    } catch (const std::bad_alloc & e) {
        return "Out of memory";
    } catch (const std::exception & e) {
        GetExceptionMessage(e, ss, 0);
    }

    try {
        buffer = ss.str();
    } catch (const std::bad_alloc & e) {
        return "Out of memory";
    }

    return buffer.c_str();
}

}
}

