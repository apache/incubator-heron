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

////////////////////////////////////////////////////////////////////////////////
//
// Type definitions for commonly used data types to ensure portability
//
///////////////////////////////////////////////////////////////////////////////

#if !defined(__SP_TYPES_H)
#define __SP_TYPES_H

#include <sys/types.h>
#include <string>
#include <limits>

typedef int8_t sp_int8;
typedef u_int8_t sp_uint8;

typedef int16_t sp_int16;
typedef u_int16_t sp_uint16;

typedef int32_t sp_int32;
typedef u_int32_t sp_uint32;

// TODO(vikasr/karthik): Remove sp_lint32/sp_ulint32 usages and just use sp_int32/sp_uint32
typedef int32_t sp_lint32;
typedef u_int32_t sp_ulint32;

typedef int64_t sp_int64;
typedef u_int64_t sp_uint64;

typedef float sp_double32;
typedef double sp_double64;

#if !defined(_WINDOWS)
typedef int SOCKET;
const SOCKET INVALID_SOCKET = -1;
const SOCKET SOCKET_ERROR = -1;
const SOCKET SOCKET_SUCCESS = 0;
#else
const int SOCKET_SUCCESS = 0;
#endif

#ifdef SP_UNICODE
#ifdef SP_WORKING_LOCALE
#include <locale>
#endif
#define SP_TEXT2(STRING) L##STRING
#else
#define SP_TEXT2(STRING) STRING
#endif  // UNICODE

#define SP_TEXT(STRING) SP_TEXT2(STRING)

#ifdef SP_UNICODE

typedef wchar_t sp_char;
typedef std::wstring sp_string;

#ifdef SP_WORKING_LOCALE
std::string toString(const std::wstring &, std::locale const & = std::locale());

std::string toString(wchar_t const *, std::locale const & = std::locale());

std::wstring toWideString(const std::string &, std::locale const & = std::locale());

std::wstring toWideString(char const *, std::locale const & = std::locale());
#else   // SP_WORKING_LOCALE
std::string toString(const std::wstring &);

std::string toString(wchar_t const *);

std::wstring toWidestring(const std::string &);

std::wstring toWidestring(char const *);
#endif  // SP_WORKING_LOCALE

#define SP_C_STR_TO_SPSTRING(STRING) ::toWideString(STRING)
#define SP_STRING_TO_SPSTRING(STRING) ::toWideString(STRING)
#define SP_SPSTRING_TO_STRING(STRING) ::toString(STRING)

#else  // SP_UNICODE

typedef char sp_char;
typedef unsigned char sp_uchar;
typedef std::string sp_string;

#define SP_C_STR_TO_SPSTRING(STRING) std::string(STRING)
#define SP_STRING_TO_SPSTRING(STRING) STRING
#define SP_SPSTRING_TO_STRING(STRING) STRING

#endif  // SP_UNICODE

const std::string EMPTY_STRING = ""; // NOLINT

#endif /* end of header file */
