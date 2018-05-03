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

#include "basics/strutils.h"
#include <cstring>
#include <string>
#include <sstream>
#include <vector>

std::vector<std::string>
StrUtils::split(
  const std::string&         input,
  const std::string&         delim) {
  size_t                    start_pos = 0, pos = 0;
  std::string               atoken;
  std::vector<std::string>  tokens;

  while ((pos = input.find(delim, start_pos)) != std::string::npos) {
    atoken = input.substr(start_pos, pos - start_pos);
    tokens.push_back(atoken);
    start_pos = pos + delim.length();
  }

  if (input.size() > start_pos) {
    atoken = input.substr(start_pos, std::string::npos);
    tokens.push_back(atoken);
  }

  return tokens;
}
