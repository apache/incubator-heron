/*
 * Copyright 2017 Twitter, Inc.
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

#ifndef HERON_API_LOGGER_LOGGER_H_
#define HERON_API_LOGGER_LOGGER_H_

namespace heron {
namespace api {
namespace logger {

class Logger {
 public:
  virtual ~Logger() { }
  template<typename T>
  virtual std::unique_ptr<Logger> operator<<(const T& stuff) = 0;
};

}  // namespace logger
}  // namespace api
}  // namespace heron

#endif  // HERON_API_LOGGER_LOGGER_H_
