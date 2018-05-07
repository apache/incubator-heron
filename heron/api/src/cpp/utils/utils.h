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

#ifndef HERON_API_UTILS_UTILS_H_
#define HERON_API_UTILS_UTILS_H_

#include <string>
#include <map>
#include <vector>
#include <cstdlib>

namespace heron {
namespace api {
namespace utils {

class Utils {
 public:
  static void readCommandLineOpts(std::map<std::string, std::string>& opts) {
    char* env = std::getenv("HERON_OPTIONS");
    if (!env) return;
    std::string options(env);
    replaceString(options, "%%%%", " ");
    std::vector<std::string> configs;
    splitString(options, ',', configs);
    for (auto config : configs) {
      std::vector<std::string> option;
      splitString(config, '=', option);
      if (option.size() == 2) {
        opts[option[0]] = option[1];
      }
    }
  }

  static void replaceString(std::string& str,
                                 const std::string& oldStr,
                                 const std::string& newStr) {
    std::string::size_type pos = 0u;
    while ((pos = str.find(oldStr, pos)) != std::string::npos) {
       str.replace(pos, oldStr.length(), newStr);
       pos += newStr.length();
    }
  }

  static void splitString(const std::string& str, char c, std::vector<std::string>& ret) {
    std::string::size_type pos = 0u;
    std::string::size_type newPos = 0u;
    while ((newPos = str.find(c, pos)) != std::string::npos) {
      ret.push_back(str.substr(pos, newPos - pos));
      pos = newPos + 1;
    }
    if (pos < str.size()) {
      ret.push_back(str.substr(pos, str.size() - pos));
    }
  }
  static const std::string DEFAULT_STREAM_ID;
};

}  // namespace utils
}  // namespace api
}  // namespace heron

#endif  // HERON_API_UTILS_UTILS_H_
