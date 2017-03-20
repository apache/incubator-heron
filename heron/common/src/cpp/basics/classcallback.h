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

#ifndef HERON_CLASS_CALLBACK_H_
#define HERON_CLASS_CALLBACK_H_

#include <functional>
#include "basics/callback.h"

template <typename C, typename... T>
CallBack* CreateCallback(C* c, void (C::*cb)(T...), T... args) {
  return new CallBack(false, std::bind(cb, c, args...));
}

template <typename C, typename... T>
CallBack* CreatePersistentCallback(C* c, void (C::*cb)(T...), T... args) {
  return new CallBack(true, std::bind(cb, c, args...));
}

#endif  // HERON_CLASS_CALLBACK_H_
