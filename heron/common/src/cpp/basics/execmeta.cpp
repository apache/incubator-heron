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

////////////////////////////////////////////////////////////////////
//
// Defines a class for holding several meta data of the program
//
/////////////////////////////////////////////////////////////////////

#include "basics/execmeta.h"
#include <string>

namespace heron {
namespace common {

const std::string& ExecutableMetadata::name() const { return name_; }

ExecutableMetadata& ExecutableMetadata::setName(const char* argv0) {
  name_ = std::string(argv0);
  return *this;
}

const std::string& ExecutableMetadata::instance() const { return instance_; }

ExecutableMetadata& ExecutableMetadata::setInstance(const char* instance) {
  instance_ = std::string(instance);
  return *this;
}

const std::string& ExecutableMetadata::package() const { return package_; }

ExecutableMetadata& ExecutableMetadata::setPackage(const char* package) {
  package_ = std::string(package);
  return *this;
}

const std::string& ExecutableMetadata::version() const { return version_; }

ExecutableMetadata& ExecutableMetadata::setVersion(const char* version) {
  version_ = std::string(version);
  return *this;
}

const std::string& ExecutableMetadata::majorVersion() const { return major_; }

ExecutableMetadata& ExecutableMetadata::setMajorVersion(const char* major) {
  major_ = std::string(major);
  return *this;
}

const std::string& ExecutableMetadata::minorVersion() const { return minor_; }

ExecutableMetadata& ExecutableMetadata::setMinorVersion(const char* minor) {
  minor_ = std::string(minor);
  return *this;
}

const std::string& ExecutableMetadata::patchNumber() const { return patch_; }

ExecutableMetadata& ExecutableMetadata::setPatchNumber(const char* patch) {
  patch_ = std::string(patch);
  return *this;
}

const std::string& ExecutableMetadata::compileUser() const { return compile_user_; }

ExecutableMetadata& ExecutableMetadata::setCompileUser(const char* user) {
  compile_user_ = std::string(user);
  return *this;
}

const std::string& ExecutableMetadata::compileHost() const { return compile_host_; }

ExecutableMetadata& ExecutableMetadata::setCompileHost(const char* host) {
  compile_host_ = std::string(host);
  return *this;
}

const std::string& ExecutableMetadata::compileTime() const { return compile_time_; }

ExecutableMetadata& ExecutableMetadata::setCompileTime(const char* time) {
  compile_time_ = std::string(time);
  return *this;
}

const std::string& ExecutableMetadata::gitSha() const { return git_sha_; }

ExecutableMetadata& ExecutableMetadata::setGitSha(const char* git_sha) {
  git_sha_ = std::string(git_sha);
  return *this;
}

const std::string& ExecutableMetadata::gitBranch() const { return git_branch_; }

ExecutableMetadata& ExecutableMetadata::setGitBranch(const char* git_branch) {
  git_branch_ = std::string(git_branch);
  return *this;
}

const std::time_t& ExecutableMetadata::startTime() const { return start_time_; }

ExecutableMetadata& ExecutableMetadata::setStartTime(const std::time_t& time) {
  start_time_ = time;
  return *this;
}

const std::string& ExecutableMetadata::logPrefix() const { return log_prefix_; }

ExecutableMetadata& ExecutableMetadata::setLogPrefix(const char* log_prefix) {
  log_prefix_ = std::string(log_prefix);
  return *this;
}

const std::string& ExecutableMetadata::logDirectory() const { return log_directory_; }

ExecutableMetadata& ExecutableMetadata::setLogDirectory(const char* log_directory) {
  log_directory_ = std::string(log_directory);
  return *this;
}

bool ExecutableMetadata::unitTest() const { return unit_test_; }

ExecutableMetadata& ExecutableMetadata::setUnitTest(const bool unit_test) {
  unit_test_ = unit_test;
  return *this;
}
}  // namespace common
}  // namespace heron
