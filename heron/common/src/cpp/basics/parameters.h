// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef PARAMS_H
#define PARAMS_H

#include <cstdint>
#include <algorithm>
#include <unordered_map>
#include <string>

namespace heron {
namespace common {

/**
 * Parameters is a container if you want to pass several parameters to a 
 * function or class method
 */
class Parameters {
 public:
  Parameters() { }
  virtual ~Parameters() { map_.clear(); }

  Parameters(const Parameters& _params) {
    this->map_.clear();
    this->map_.insert(_params.map_.begin(), _params.map_.end());
  }

  Parameters& operator = (const Parameters& _params) {
    this->map_.clear();
    this->map_.insert(_params.map_.begin(), _params.map_.end());
    return *this;
  }

  int size() {
    return map_.size();
  }

  // get the value associated with the key as string
  std::string getstr(const std::string& key) const {
    return get(key);
  }

  std::string getstr(const std::string& key, const std::string& defaults) {
    std::string value = getstr(key);
    return !value.empty() ? value : defaults;
  }

  // get the value associated with the key as bool
  bool getbool(const std::string& key) {
    std::string value = get(key);
    if (value.empty())
      return false;
    std::transform(value.begin(), value.end(), value.begin(), toupper);
    return value == "TRUE" ? true : false;
  }

  bool getbool(const std::string& key, bool defaults) {
    std::string value = get(key);
    if (value.empty())
      return defaults;
    std::transform(value.begin(), value.end(), value.begin(), toupper);
    return value == "TRUE" ? true : false;
  }

  // get the value associated with the key as long
  int64_t getint64(const std::string& key) {
    std::string value = get(key);
    return std::stol(value, nullptr);
  }

  int64_t getint64(const std::string& key, int64_t defaults) {
    std::string value = get(key);
    if (value.empty()) return defaults;
    return std::stol(value, nullptr);
  }

  // get the value associated with the key as integer
  int32_t getint32(const std::string& key) {
    std::string value = get(key);
    return std::stoi(value, nullptr);
  }

  int32_t getint32(const std::string& key, int32_t defaults) {
    std::string value = get(key);
    if (value.empty()) return defaults;
    return std::stoi(value, nullptr);
  }

  // get the value associated with the key as double
  double getdouble(const std::string& key) {
    std::string value = get(key);
    return std::stod(value, nullptr);
  }

  double getdouble(const std::string& key, double defaults) {
    std::string value = get(key);
    if (value.empty()) return defaults;
    return std::stod(value, nullptr);
  }

  void* getptr(const std::string& key) {
    std::string value = get(key);
    if (value.empty()) return nullptr;
    int64_t ptr = std::stol(value, nullptr);
    return reinterpret_cast<void *>(ptr);
  }

  // check if the key exists or not
  bool contains(const std::string& key) {
    auto got = map_.find(key);
    return got != map_.end() ? true : false;
  }

  // get the value associated with the key.
  // if does not exist return empty string
  std::string get(const std::string& key) const {
    auto got = map_.find(key);
    return got != map_.end() ? got->second : std::string();
  }

  // builder class for adding key values
  class Builder {
   public:
    Builder() { }

    Builder(const Builder& _builder) {
      this->map_.clear();
      this->map_.insert(_builder.map_.begin(), _builder.map_.end());
    }

    Builder& operator = (const Builder& _builder) {
      this->map_.clear();
      this->map_.insert(_builder.map_.begin(), _builder.map_.end());
      return *this;
    }

    // put a string value associated with the key
    Builder& putstr(const std::string& key, const std::string& value) {
      map_.emplace(key, value);
      return *this;
    }

    // put a bool value associated with the key
    Builder& putbool(const std::string& key, bool value) {
      std::string bval = value ? "TRUE" : "FALSE";
      map_.emplace(key, bval);
      return *this;
    }

    // put a long value associated with the key
    Builder& putint64(const std::string& key, int64_t value) {
      map_.emplace(key, std::to_string(value));
      return *this;
    }

    // put an int value associated with the key
    Builder& putint32(const std::string& key, int32_t value) {
      map_.emplace(key, std::to_string(value));
      return *this;
    }

    // put a double value associated with the key
    Builder& putdouble(const std::string& key, double value) {
      map_.emplace(key, std::to_string(value));
      return *this;
    }

    // put a ptr value associated with the key
    Builder& putptr(const std::string& key, void* value) {
      map_.emplace(key, std::to_string(reinterpret_cast<int64_t>(value)));
      return *this;
    }

    Builder& putall(const Builder& builder) {
      map_.insert(builder.map_.begin(), builder.map_.end());
      return *this;
    }

    Parameters build() {
      return Parameters(*this);
    }

   private:
    std::unordered_map<std::string, std::string>  map_;
    friend class Parameters;
  };

  explicit Parameters(const Builder& builder) {
    this->map_.clear();
    this->map_.insert(builder.map_.begin(), builder.map_.end());
  }

 private:
  std::unordered_map<std::string, std::string> map_;
};

}  // namespace common
}  // namespace heron

#endif
