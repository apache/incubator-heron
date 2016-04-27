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

#include "basics/classcallback.h"

/*
 * Defines Callback for class member functions that take no arguments
 */
template <typename C>
class ClassCallBack_0 : public CallBack {
 public:
  ClassCallBack_0(bool persist, C* c, void (C::*cb)()) : CallBack(persist) {
    c_ = c;
    cb_ = cb;
  }
  virtual ~ClassCallBack_0() {}
  virtual void RunCallback() { (c_->*cb_)(); }

 private:
  C* c_;             // instance of the class
  void (C::*cb_)();  // function ptr to class method
};

/*
 * Defines callback for class member functions that take 1 arg
 */
template <typename C, typename D>
class ClassCallBack_1 : public CallBack {
 public:
  ClassCallBack_1(bool persist, C* c, void (C::*cb)(D), D d) : CallBack(persist) {
    c_ = c;
    cb_ = cb;
    d_ = d;
  }
  virtual ~ClassCallBack_1() {}
  virtual void RunCallback() { (c_->*cb_)(d_); }

 private:
  C* c_;              // instance of the class
  void (C::*cb_)(D);  // function ptr to class method
  D d_;               // actual value of the arg
};

/*
 * Defines callback for class member functions that take 2 args
 */
template <typename C, typename D, typename E>
class ClassCallBack_2 : public CallBack {
 public:
  ClassCallBack_2(bool persist, C* c, void (C::*cb)(D, E), D d, E e) : CallBack(persist) {
    c_ = c;
    cb_ = cb;
    d_ = d;
    e_ = e;
  }
  virtual ~ClassCallBack_2() {}
  virtual void RunCallback() { (c_->*cb_)(d_, e_); }

 private:
  C* c_;                 // instance of the class
  void (C::*cb_)(D, E);  // function ptr to class method
  D d_;                  // actual value of the 1st arg
  E e_;                  // actual value of the 2nd arg
};

/*
 * Defines callback for class member functions that take 3 arguments
 */
template <typename C, typename D, typename E, typename F>
class ClassCallBack_3 : public CallBack {
 public:
  ClassCallBack_3(bool persist, C* c, void (C::*cb)(D, E, F), D d, E e, F f) : CallBack(persist) {
    c_ = c;
    cb_ = cb;
    d_ = d;
    e_ = e;
    f_ = f;
  }
  virtual ~ClassCallBack_3() {}
  virtual void RunCallback() { (c_->*cb_)(d_, e_, f_); }

 private:
  C* c_;                    // instance of the class
  void (C::*cb_)(D, E, F);  // function ptr to class method
  D d_;                     // actual value of the 1st arg
  E e_;                     // actual value of the 2nd arg
  F f_;                     // actual value of the 3rd arg
};

template <typename C>
CallBack* CreateCallback(C* c, void (C::*cb)()) {
  auto cb0 = new ClassCallBack_0<C>(false, c, cb);
  return cb0;
}

template <typename C, typename D>
CallBack* CreateCallback(C* c, void (C::*cb)(D), D d) {
  auto cb1 = new ClassCallBack_1<C, D>(false, c, cb, d);
  return cb1;
}

template <typename C, typename D, typename E>
CallBack* CreateCallback(C* c, void (C::*cb)(D, E), D d, E e) {
  auto cb2 = new ClassCallBack_2<C, D, E>(false, c, cb, d, e);
  return cb2;
}

template <typename C, typename D, typename E, typename F>
CallBack* CreateCallback(C* c, void (C::*cb)(D, E, F), D d, E e, F f) {
  auto cb3 = new ClassCallBack_3<C, D, E, F>(false, c, cb, d, e, f);
  return cb3;
}

template <typename C>
CallBack* CreatePersistentCallback(C* c, void (C::*cb)()) {
  auto cb0 = new ClassCallBack_0<C>(true, c, cb);
  return cb0;
}

template <typename C, typename D>
CallBack* CreatePersistentCallback(C* c, void (C::*cb)(D), D d) {
  auto cb1 = new ClassCallBack_1<C, D>(true, c, cb, d);
  return cb1;
}

template <typename C, typename D, typename E>
CallBack* CreatePersistentCallback(C* c, void (C::*cb)(D, E), D d, E e) {
  auto cb2 = new ClassCallBack_2<C, D, E>(true, c, cb, d, e);
  return cb2;
}

template <typename C, typename D, typename E, typename F>
CallBack* CreatePersistentCallback(C* c, void (C::*cb)(D, E, F), D d, E e, F f) {
  auto cb3 = new ClassCallBack_3<C, D, E, F>(true, c, cb, d, e, f);
  return cb3;
}

#endif  // HERON_CLASS_CALLBACK_H_
