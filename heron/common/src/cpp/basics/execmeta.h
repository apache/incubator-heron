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

#if !defined(HERON_EXEC_META_H)
#define HERON_EXEC_META_H

#include <chrono>
#include <string>
#include "basics/sptypes.h"

namespace heron {
namespace common {

/**
 * This class contains several meta data of the program. Metadata includes
 *  name of the program
 *  instance id of the program
 *  name of the package
 *  major, minor and path version of the program
 *  host the program was compiled
 *  user who compiled the program
 *  time of compilation
 *  git branch and sha
 *
 */

class ExecutableMetadata {
 public:
  ExecutableMetadata() {}

  // get & set the name of the program
  const std::string& name() const;
  ExecutableMetadata& setName(const char* argv0);

  // get & set the instance id of the program
  const std::string& instance() const;
  ExecutableMetadata& setInstance(const char* instance);

  // get & set the name of the package
  const std::string& package() const;
  ExecutableMetadata& setPackage(const char* package);

  // get & set the version of the program
  const std::string& version() const;
  ExecutableMetadata& setVersion(const char* version);

  // get & set the major version of the program
  const std::string& majorVersion() const;
  ExecutableMetadata& setMajorVersion(const char* major);

  // get & set the minor version of the program
  const std::string& minorVersion() const;
  ExecutableMetadata& setMinorVersion(const char* minor);

  // get & set the patch of the program
  const std::string& patchNumber() const;
  ExecutableMetadata& setPatchNumber(const char* patch);

  // get & set the user of compilation
  const std::string& compileUser() const;
  ExecutableMetadata& setCompileUser(const char* user);

  // get & set the user of host
  const std::string& compileHost() const;
  ExecutableMetadata& setCompileHost(const char* host);

  // get & set the time of compilation of the program
  const std::string& compileTime() const;
  ExecutableMetadata& setCompileTime(const char* time);

  // get & set the git sha of the build
  const std::string& gitSha() const;
  ExecutableMetadata& setGitSha(const char* git_sha);

  // get & set the git branch of the build
  const std::string& gitBranch() const;
  ExecutableMetadata& setGitBranch(const char* git_branch);

  // get & set the time of starting of the program
  const std::time_t& startTime() const;
  ExecutableMetadata& setStartTime(const std::time_t& time);

  // get & set the log prefix
  const std::string& logPrefix() const;
  ExecutableMetadata& setLogPrefix(const char* log_prefix);

  // get & set the log directory
  const std::string& logDirectory() const;
  ExecutableMetadata& setLogDirectory(const char* log_directory);

  // get & set if the program is a unit test
  bool unitTest() const;
  ExecutableMetadata& setUnitTest(const bool unittest);

 private:
  /// Name of the program.
  std::string name_;

  /// Instance of the program. Will be empty for singleton
  /// programs and tests.
  std::string instance_;

  /// Package name.
  std::string package_;

  /// Version string of the package.
  std::string version_;

  /// Major version string of the package.
  std::string major_;

  /// Minor version string of the package.
  std::string minor_;

  /// Patch number of the package.
  std::string patch_;

  /// Unix user name who compiled the program.
  std::string compile_user_;

  /// Name of the host on which it is compiled.
  std::string compile_host_;

  /// Time of compilation.
  std::string compile_time_;

  /// Name of the git branch.
  std::string git_branch_;

  /// Git SHA of the branch.
  std::string git_sha_;

  /// Start time of the program during execution.
  std::time_t start_time_;

  /// Prefix to use for log files.
  std::string log_prefix_;

  /// Directory name used for log files.
  std::string log_directory_;

  /// Flag to indicate whether the program is a test
  bool unit_test_;
};
}  // namespace common
}  // namespace heron

#endif  // execmeta.h
