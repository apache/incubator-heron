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
#ifndef _HDFS_LIBHDFS3_MOCK_TESTUTIL_H_
#define _HDFS_LIBHDFS3_MOCK_TESTUTIL_H_

#include <cstddef>
#include <cassert>

#include "Logger.h"

namespace Hdfs {

/**
 * Fill the buffer with "012345678\n".
 * @param buffer The buffer to be filled.
 * @param size The size of the buffer.
 * @param offset Start offset of the content to be filled.
 */
static inline void FillBuffer(char * buffer, size_t size, size_t offset) {
	int64_t todo = size;

	char c;
	while (todo-- > 0) {
		c = offset++ % 10;
		c = c < 9 ? c + '0' : '\n';
		*buffer++ = c;
	}
}

/**
 * Check the content of buffer if it is filled with "012345678\n"
 * @param buffer The buffer to be checked.
 * @param size The size of buffer.
 * @param offset Start offset of the content in buffer.
 * @return Return true if the content of buffer is filled with expected data.
 */
static inline bool CheckBuffer(const char * buffer, size_t size,
		size_t offset) {
	int64_t todo = size;

	char c;
	while (todo-- > 0) {
		c = offset++ % 10;
		c = c < 9 ? c + '0' : '\n';
		if (*buffer++ != c) {
			return false;
		}
	}

	return true;
}

static inline const char * GetEnv(const char * key, const char * defaultValue) {
	const char * retval = getenv(key);
	if (retval && strlen(retval) > 0) {
		return retval;
	}
	return defaultValue;
}

}

#define DebugException(function) \
    try { \
        function ; \
    } catch (const Hdfs::HdfsException & e) { \
        std::string buffer; \
        LOG(LOG_ERROR, "DEBUG:\n%s", Hdfs::Internal::GetExceptionDetail(e, buffer)); \
        throw; \
    } catch (const std::exception & e) { \
        LOG(LOG_ERROR, "DEBUG:\n%s", e.what()); \
        throw; \
    }

#endif
