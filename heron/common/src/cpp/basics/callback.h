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

////////////////////////////////////////////////////////////////////////////////
//
// Generic callback definitions. In their simplest form they can be used as an
// alternative to the
//
//    void (*func)()
//
// counterparts of C.
//
// More interesting however is their usage when functions expect arguments.
//
// Lets say that you want to pass a function to a routine that needs to execute
// it once when certain conditions are met. In typical C, you would do something
// like
//
//   void Routine(int arg, int arg2, void (*func)())
//   {
//      ... some code ...
//      func();
//   }
//
// This approach has two issues -
//    - what if you don't know the signature of the function?
//    - What if func can take one or more arguments?
//
// In such cases, using our Callbacks you can do the following
//
//   void Routine(int arg, int arg2, CallBack* a)
//   {
//     ... some code ...
//     a->Run();
//   }
//
// Now when you actually pass on the Callback during invocation, you can do
// something like
//
//   CallBack* cb = CreateCallback(void (*func)());
//
//    or
//
//   CallBack* cb = CreateCallback(void (*func)(int), 10);
//   Routine(arg, arg2, cb);
//
// Routine doesn't have to know the signature of the function, nor does it need
// to remember any arguments that the function requires. The CallBack cb is
// destroyed after its invocation so one doesn't need to worry abt cleaning
// it up.
////////////////////////////////////////////////////////////////////////////////

#ifndef HERON_CALLBACK_H_
#define HERON_CALLBACK_H_

#include <functional>

/**
 * Basic CallBack definition.
 * There are two types of CallBacks.
 *   - Temporary callback - the CallBack gets deleted as soon as its invoked.
 *   - Persistent callBack - the CallBack remains across multiple invocations.
 */
class CallBack {
 public:
  // Constructor. Users should use the CreateCallback functions detailed below.
  CallBack(bool persist, std::function<void()> f)
    : persistent_(persist), bind_(f) {}

  virtual ~CallBack() {}

  // The main exported function
  void Run() {
    // In case of persistent callbacks, the RunCallback might delete itself.
    // In that case after RunCallback returns it is not safe to access
    // the class member persistent_. So make a copy now.
    bool persist = persistent_;
    bind_();
    if (!persist) {
      delete this;
    }
  }

  // Return whether the callback is temporary or permanent.
  bool isPersistent() const { return persistent_; }

  void makePersistent() { persistent_ = true; }

 private:
  bool persistent_;
  std::function<void()> bind_;
};

//
// Helper functions to create callbacks
//
template <typename... C>
inline
CallBack* CreateCallback(void (*cb)(C... args), C... args) {
  return new CallBack(false, std::bind(cb, args...));
}

template <typename... C>
inline
CallBack* CreatePersistentCallback(void (*cb)(C... args), C... args) {
  return new CallBack(true, std::bind(cb, args...));
}

#endif  // HERON_CALLBACK_H_
