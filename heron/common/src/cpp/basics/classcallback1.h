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

#ifndef HERON_CLASS_CALLBACK1_H_
#define HERON_CLASS_CALLBACK1_H_

#include <functional>
#include "basics/callback1.h"

template <typename C, typename D, typename... T>
CallBack1<D>* CreateCallback(C* c, void (C::*cb)(D, T...), T... args) {
  return new CallBack1<D>(false, std::bind(cb, c, std::placeholders::_1, args...));
}

template <typename C, typename D, typename... T>
CallBack1<D>* CreatePersistentCallback(C* c, void (C::*cb)(D, T...), T... args) {
  return new CallBack1<D>(true, std::bind(cb, c, std::placeholders::_1, args...));
}

#endif  // HERON_CLASS_CALLBACK1_H_
