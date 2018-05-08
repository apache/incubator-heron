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

#ifndef HERON_CALLBACK1_H_
#define HERON_CALLBACK1_H_

#include <functional>

//
// Sometimes we might want to pass some status or data to the functions being invoked.
// In that case we create CallBack1 instead of a CallBack and do something like this
//   void Routine(int arg, int arg2, CallBack1<int>* cb) {
//     ... some code ...
//     cb->Run(arg2);
//   }
// This way Routine can pass status/data to the CallBack cb without having to worry abt
// its signature or its associated arguments.
// Users can invoke Routine in the following manner
// CallBack1<int>* cb = CreateCallback(void (*func)(int));
//    or
// CallBack1<int> cb = CreateCallback(void (*func)(int, char), 'c');
// Routine(arg, arg2, cb);

template <typename C>
class CallBack1 {
 public:
  // Constructor. Users should use the CreateCallback functions detailed below.
  CallBack1(bool persist, std::function<void(C)> f)
    : persistent_(persist), bind_(f) {}

  virtual ~CallBack1() {}

  // The main exported function
  void Run(C c) {
    // In case of persistent callbacks, the RunCallback might delete itself.
    // In that case after RunCallback returns it is not safe to access
    // the class member persistent_. So make a copy now.
    bool persist = persistent_;
    bind_(c);
    if (!persist) {
      delete this;
    }
  }

  // Return whether the callback is temporary or permanent.
  bool isPersistent() const { return persistent_; }

 private:
  bool persistent_;
  std::function<void(C)> bind_;
};

template <typename C, typename... T>
CallBack1<C>* CreateCallback(void (*cb)(C, T...), T... args) {
  return new CallBack1<C>(false, std::bind(cb, std::placeholders::_1, args...));
}

#endif  // HERON_CLASS_CALLBACK1_H_
