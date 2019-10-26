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
// Some helper functions regarding for file and directory operations
//
///////////////////////////////////////////////////////////////////////////////

#if !defined(HERON_FILE_UTILS_H_)
#define HERON_FILE_UTILS_H_

#include <string>
#include <vector>
#include "basics/sptypes.h"

class FileUtils {
 public:
  //! get the basename of the given file
  static std::string baseName(const std::string& path);

  //! creates a directory if it does not exist
  static sp_int32 makeDirectory(const std::string& dir);

  //! removes a file given an absolute path
  static sp_int32 removeFile(const std::string& filepath);

  //! recursively remove files in a directory
  static sp_int32 removeRecursive(const std::string& dir, bool delete_self);

  //! lists all the files in a directory
  static sp_int32 listFiles(const std::string& dir, std::vector<std::string>& files);

  //! reads all contents of the file and return it
  static std::string readAll(const std::string& filename);

  //! gets the last modified time of this file
  static time_t getModifiedTime(const std::string& filename);

  //! check if the file is a symlink, given the absolute path
  static bool is_symlink(const std::string& filepath);

  //! write the contents of a string into the file
  static bool writeAll(const std::string& filename, const char* data, size_t len);

  //! get the current working directory
  static sp_int32 getCwd(std::string& path);
};

#endif
