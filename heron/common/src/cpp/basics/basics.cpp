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

#include <signal.h>
#include <stdlib.h>

#include <chrono>
#include <cstddef>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "config/heron-config.h"

#include "basics/basics.h"
#include "basics/execmeta.h"

namespace heron {
namespace common {

static ExecutableMetadata gExecMeta;

/**
 * Set the execution metadata for the program. Several values are
 * populated including
 *
 *   - name and instance of the program, if any
 *   - whether it is a unit test
 *   - user who compiled, host of compilation and time of compilation
 *   - name of the package and its version
 *
 * \param argv0
 *      Name of the program calling initialize
 *
 * \param instance
 *      Instance id of the program calling initialize
 *
 * \param testing
 *      To indicate whether the program is a unit test
 *
 */
static void SetMetadata(const char* argv0, const char* instance, bool testing) {
  // set the name of the program and its instance
  gExecMeta.setName(argv0);
  gExecMeta.setInstance(instance != nullptr ? instance : "0");

  // set if it is a unit test
  gExecMeta.setUnitTest(testing);

  // set the compile user, host and time
  gExecMeta.setCompileUser(PACKAGE_COMPILE_USER);
  gExecMeta.setCompileHost(PACKAGE_COMPILE_HOST);
  gExecMeta.setCompileTime(PACKAGE_COMPILE_TIME);

  // set the start time of compilation
  auto start = std::chrono::system_clock::now();
  gExecMeta.setStartTime(std::chrono::system_clock::to_time_t(start));

  // set the package name and version
  gExecMeta.setPackage(PACKAGE_NAME);
  gExecMeta.setVersion(PACKAGE_VERSION);

  // set the major and minor version
  std::string sversion(PACKAGE_VERSION);
  std::string smajor(sversion, 0, sversion.find_first_of("."));

  std::string sminor(sversion, sversion.find_first_of(".") + 1,
                     sversion.find_last_of(".") - sversion.find_first_of("."));

  gExecMeta.setMajorVersion(smajor.c_str());
  gExecMeta.setMinorVersion(sminor.c_str());

  // set the patch number
  std::string patch(sversion, sversion.find_last_of(".") + 1);
  gExecMeta.setPatchNumber(patch.c_str());
}

/**
 * Function to initialize logging. It is used by other initialization
 * functions. During initialization, it sets several values such as
 *
 *   - test log directory, if the program is a test
 *   - log directory if it is actual Heron program
 *   - set the log file prefix and log directory
 *   - set the maximum size of the log file
 *   - install signal handler for processing SIGSEGV
 *
 */
static void InitLogging() {
  // set the log directory
  gExecMeta.setLogDirectory(gExecMeta.unitTest() ? constTestLogsDirectory : constLogsDirectory);

  // get the basename of the file path
  std::string bname = FileUtils::baseName(gExecMeta.name());

  // form the log file prefix
  std::string log_prefix(bname);
  log_prefix.append("-").append(gExecMeta.instance());

  // finally set the log file prefix
  gExecMeta.setLogPrefix(log_prefix.c_str());

  // configure glog parameters
  FLAGS_stderrthreshold = 3;  // FATAL

  // set the max log size to 100MB
  FLAGS_max_log_size = 100;

  // set the default logging directory
  FLAGS_log_dir = gExecMeta.logDirectory();

  // set the default flush interval to 10 seconds
  FLAGS_logbufsecs = 10;

  // enable logging only in the INFO file. This will contain
  // all messages >=INFO Severity
  for (google::LogSeverity s = google::WARNING; s < google::NUM_SEVERITIES; s++)
    google::SetLogDestination(s, "");

  // init the logging
  google::InitGoogleLogging(gExecMeta.logPrefix().c_str());

  // do an initial prune, in case, if there are some files to trim
  PruneLogs();

  // install the google signal handler for SIGSEGV
  google::InstallFailureSignalHandler();
}

/**
 * Helper function to initialize a Heron program. It is used by
 * It initializes the execution metadata, logs, etc. It is the
 * first function to be called in main()
 *
 * \param argv0
 *      Name of the program calling initialize
 *
 * \param instance
 *      Instance id of the program calling initialize
 */
static void InitHelper(const char* argv0, const char* instance, bool istest) {
  CHECK(signal(SIGPIPE, SIG_IGN) != SIG_ERR);

  // create execution meta data object
  SetMetadata(argv0, instance, istest);

  // init the logging
  InitLogging();

  // init the random number system
  ::srand(time(nullptr));
}

/**
 * Function to initialize programs that have multiple instances.
 * It initializes the execution metadata, logging framework, etc.
 * It is the first function to be called in main()
 *
 * \param argv0
 *      Name of the program calling initialize
 *
 * \param instance
 *      Instance id of the program calling initialize
 */
void Initialize(const char* argv0, const char* instance) { InitHelper(argv0, instance, false); }

/**
 * Function to initialize singleton programs. It initializes the
 * execution metadata, logs, etc. It is the first function to be
 * called in main()
 *
 * \param argv0
 *      Name of the program calling initialize
 */
void Initialize(const char* argv0) {
  std::string prog(argv0);

  // use a different initializer depending on if it a unit test program?
  if (prog.rfind("_unittest") != std::string::npos) {
    InitHelper(argv0, nullptr, true);
  } else {
    InitHelper(argv0, nullptr, false);
  }

  LOG(INFO) << "Starting " << gExecMeta.name() << " " << gExecMeta.package() << " "
            << "v" << gExecMeta.version() << " " << gExecMeta.compileUser() << "@"
            << gExecMeta.compileHost() << " on " << gExecMeta.compileTime() << std::endl;
}

void Shutdown() { google::ShutdownGoogleLogging(); }

/**
 * Utility function to prune the log files
 */
void PruneLogs() {
  std::vector<std::string> files;
  std::map<time_t, std::string> ordered_files;

  // get the log prefix and files in the log directory
  const std::string& file_prefix = gExecMeta.logPrefix();
  FileUtils::listFiles(gExecMeta.logDirectory(), files);

  // find the files that contain the give prefix
  for (size_t i = 0; i < files.size(); ++i) {
    if (files[i].find(file_prefix) != std::string::npos) {
      // form the full file path
      std::string filePath(gExecMeta.logDirectory());
      filePath.append("/").append(files[i]);

      // ignore if it a sym link file
      if (FileUtils::is_symlink(filePath)) continue;

      // get the last time of modification
      time_t tmodified = FileUtils::getModifiedTime(filePath);

      // insert into the set of files to be examined
      std::pair<time_t, std::string> logfile;
      logfile = make_pair(tmodified, filePath);
      ordered_files.insert(logfile);
    }
  }

  // remove the files that are old
  while (ordered_files.size() > constMaxNumLogFiles) {
    FileUtils::removeFile(ordered_files.begin()->second);
    LOG(INFO) << "Pruned log file " << ordered_files.begin()->second;
    ordered_files.erase(ordered_files.begin());
  }
}

void FlushLogs() { google::FlushLogFiles(google::INFO); }
}  // namespace common
}  // namespace heron
