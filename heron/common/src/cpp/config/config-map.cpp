/*
 * Copyright 2015 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "config/config-map.h"

#include <string>
#include <vector>
#include <unordered_map>

namespace heron {
namespace config {

Config
Config::expand() {
  heron::config::Config::Builder builder;
  auto config_map = params_.getmap();
  bool any_change = false;
  do {
    for (auto kv = config_map.begin(); kv != config_map.end(); kv++) {
      const std::string value = substitute(kv->second);
      LOG(INFO) << "Value " << kv->second << " is changed to " << value;
      any_change = value != kv->second ? true : false;
      builder.putstr(kv->first, value);
    }
  } while (any_change);

  return builder.build();
}

std::string
Config::substitute(const std::string& _value) {
  auto trimmed = StrUtils::trim(_value);

  // split the trimmed string using path separator - in case if it is a path
  auto path = StrUtils::split(trimmed, "/");

  for (auto elem = path.begin(); elem != path.end(); elem++) {
    if (*elem == "${HOME}") {
      *elem = FileUtils::getHomeDirectory();
      LOG_IF(FATAL, elem->empty()) << "HOME directory is empty";

    } else if (*elem == "~") {
      *elem = FileUtils::getHomeDirectory();
      LOG_IF(FATAL, elem->empty()) << "HOME directory is empty";

    } else if (*elem == "${JAVA_HOME}") {
      const char* java_path = ::getenv("JAVA_HOME");
      LOG_IF(FATAL, java_path == nullptr) << "JAVA_HOME not set";
      *elem = java_path;

    } else if (*elem == "${CLUSTER}") {
      *elem = params_.getstr(CommonConfigVars::CLUSTER);
      LOG_IF(FATAL, elem->empty()) << "CLUSTER not set";

    } else if (*elem == "${ROLE}") {
      *elem = params_.getstr(CommonConfigVars::ROLE);
      LOG_IF(FATAL, elem->empty()) << "ROLE not set";

    } else if (*elem == "${ENVIRON}") {
      *elem = params_.getstr(CommonConfigVars::ENVIRON);
      LOG_IF(FATAL, elem->empty()) << "ENVIRON not set";

    } else if (*elem == "${TOPOLOGY}") {
      *elem = params_.getstr(CommonConfigVars::TOPOLOGY_NAME);
      LOG_IF(FATAL, elem->empty()) << "TOPOLOGY NAME not set";
    }
  }

  return combine(path);
}

std::string
Config::combine(const std::vector<std::string>& paths) {
  std::string join_str(paths[0]);
  for (auto i = 1 ; i < paths.size(); i++) {
    if (paths[i].front() == '/') {
      join_str.append(paths[i]);
    } else {
      join_str.append("/").append(paths[i]);
    }
  }
  return join_str;
}

}  // namespace config
}  // namespace heron
