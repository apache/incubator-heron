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

#include "basics/fileutils.h"
#include <dirent.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include "glog/logging.h"

#include "basics/sprcodes.h"
#include "basics/spconsts.h"

std::string FileUtils::baseName(const std::string& path) {
  return path.substr(path.find_last_of(constPathSeparator) + 1);
}

sp_int32 FileUtils::makeDirectory(const std::string& directory) {
  struct stat st;

  // get the directory name stats
  auto retcode = ::stat(directory.c_str(), &st);

  // check for any errors
  if (retcode == -1) {
    if (errno == ENOENT) {
      if (::mkdir(directory.c_str(), 0700) != 0) {
        PLOG(ERROR) << "Unable to create directory " << directory;
        return SP_NOTOK;
      }
      return SP_OK;
    }

    PLOG(ERROR) << "Unable to create directory " << directory;
    return SP_NOTOK;
  }

  return S_ISDIR(st.st_mode) ? SP_OK : SP_NOTOK;
}

sp_int32 FileUtils::removeFile(const std::string& filepath) {
  if (::unlink(filepath.c_str()) != 0) {
    PLOG(ERROR) << "Unable to delete file " << filepath;
    return SP_NOTOK;
  }
  return SP_OK;
}

sp_int32 FileUtils::removeRecursive(const std::string& directory, bool delete_self) {
  auto dir = ::opendir(directory.c_str());
  if (dir == nullptr) {
    PLOG(ERROR) << "opendir failed for " << directory;
    return SP_NOTOK;
  }

  // list all dir contents
  struct dirent* entry = nullptr;
  while ((entry = ::readdir(dir)) != nullptr) {
    auto current_dir = std::string(entry->d_name).compare(constCurrentDirectory);
    auto parent_dir = std::string(entry->d_name).compare(constParentDirectory);

    if (current_dir != 0 && parent_dir != 0) {
      auto path = directory + constPathSeparator + std::string(entry->d_name);
      if (entry->d_type == DT_DIR) {
        if (removeRecursive(path, true) == SP_NOTOK) return SP_NOTOK;
      } else {
        if (::unlink(path.c_str()) != 0) {
          PLOG(ERROR) << "Unable to delete " << path;
          return SP_NOTOK;
        }
      }
    }
  }

  ::closedir(dir);

  // now delete the dir
  if (delete_self) {
    if (::rmdir(directory.c_str()) != 0) {
      PLOG(ERROR) << "Unable to delete directory " << directory;
      return SP_NOTOK;
    }
  }

  return SP_OK;
}

sp_int32 FileUtils::listFiles(const std::string& directory, std::vector<std::string>& files) {
  auto dir = ::opendir(directory.c_str());
  if (dir != nullptr) {
    struct dirent* ent;
    while ((ent = ::readdir(dir)) != nullptr) {
      auto current_dir = std::string(ent->d_name).compare(constCurrentDirectory);
      auto parent_dir = std::string(ent->d_name).compare(constParentDirectory);
      if (current_dir != 0 && parent_dir != 0) {
        files.push_back(ent->d_name);
      }
    }

    ::closedir(dir);
    return SP_OK;
  }

  // log errors that we could not open the dir
  PLOG(ERROR) << "Unable to open directory " << directory;
  return SP_NOTOK;
}

std::string FileUtils::readAll(const std::string& file) {
  std::ifstream in(file.c_str(), std::ifstream::in | std::ifstream::binary);
  std::stringstream buffer;
  buffer << in.rdbuf();
  return buffer.str();
}

time_t FileUtils::getModifiedTime(const std::string& file) {
  struct stat attrib;
  if (::stat(file.c_str(), &attrib) == 0) {
    return attrib.st_mtime;
  }

  PLOG(ERROR) << "Unable to get file modified time for " << file;
  return SP_NOTOK;
}

bool FileUtils::is_symlink(const std::string& filepath) {
  struct stat attrib;
  if (::lstat(filepath.c_str(), &attrib) == 0) {
    return S_ISLNK(attrib.st_mode);
  }

  PLOG(ERROR) << "Unable to check if file is a symlink " << filepath;
  return false;
}

bool FileUtils::writeAll(const std::string& filename, const char* data, size_t len) {
  std::ofstream ot(filename.c_str(), std::ios::out | std::ios::binary);
  if (!ot) return false;
  ot.write(reinterpret_cast<const char*>(data), len);
  ot.close();
  return true;
}

sp_int32 FileUtils::getCwd(std::string& path) {
  char maxpath[MAXPATHLEN];
  if (::getcwd(maxpath, MAXPATHLEN) == nullptr) {
    PLOG(ERROR) << "Could not get the current working directory";
    return SP_NOTOK;
  }

  path = maxpath;
  return SP_OK;
}
