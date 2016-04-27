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

#ifndef HERON_CLASS_CALLBACK1_H_
#define HERON_CLASS_CALLBACK1_H_

#include "basics/classcallback1.h"

/*
 * Defines CallBack1 for class member functions that take no arguments
 */
template <typename C, typename D>
class ClassCallBack1_0 : public CallBack1<D> {
 public:
  ClassCallBack1_0(bool persist, C* c, void (C::*cb)(D)) : CallBack1<D>(persist) {
    c_ = c;
    cb_ = cb;
  }
  virtual ~ClassCallBack1_0() {}
  virtual void RunCallback(D d) { (c_->*cb_)(d); }

 private:
  C* c_;
  void (C::*cb_)(D);
};

/*
 * Defines CallBack1 for class member functions that take 1 argument
 */
template <typename C, typename D, typename E>
class ClassCallBack1_1 : public CallBack1<E> {
 public:
  ClassCallBack1_1(bool persist, C* c, void (C::*cb)(D, E), D d) : CallBack1<E>(persist) {
    c_ = c;
    cb_ = cb;
    d_ = d;
  }
  virtual ~ClassCallBack1_1() {}
  virtual void RunCallback(E e) { (c_->*cb_)(d_, e); }

 private:
  C* c_;
  void (C::*cb_)(D, E);
  D d_;
};

/*
 * Defines CallBack1 for class member functions that take 2 arguments
 */
template <typename C, typename D, typename E, typename F>
class ClassCallBack1_2 : public CallBack1<F> {
 public:
  ClassCallBack1_2(bool persist, C* c, void (C::*cb)(D, E, F), D d, E e) : CallBack1<F>(persist) {
    c_ = c;
    cb_ = cb;
    d_ = d;
    e_ = e;
  }
  virtual ~ClassCallBack1_2() {}
  virtual void RunCallback(F f) { (c_->*cb_)(d_, e_, f); }

 private:
  void (C::*cb_)(D, E, F);
  C* c_;
  D d_;
  E e_;
};

/*
 * Defines CallBack1 for class member functions that take 3 arguments
 */
template <typename C, typename D, typename E, typename F, typename G>
class ClassCallBack1_3 : public CallBack1<G> {
 public:
  ClassCallBack1_3(bool persist, C* c, void (C::*cb)(D, E, F, G), D d, E e, F f)
      : CallBack1<G>(persist) {
    c_ = c;
    cb_ = cb;
    d_ = d;
    e_ = e;
    f_ = f;
  }
  virtual ~ClassCallBack1_3() {}
  virtual void RunCallback(G g) { (c_->*cb_)(d_, e_, f_, g); }

 private:
  void (C::*cb_)(D, E, F, G);
  C* c_;
  D d_;
  E e_;
  F f_;
};

/*
 * Defines CallBack1 for class member functions that take 4 arguments
 */
template <typename C, typename D, typename E, typename F, typename G, typename H>
class ClassCallBack1_4 : public CallBack1<H> {
 public:
  ClassCallBack1_4(bool persist, C* c, void (C::*cb)(D, E, F, G, H), D d, E e, F f, G g)
      : CallBack1<H>(persist) {
    c_ = c;
    cb_ = cb;
    d_ = d;
    e_ = e;
    f_ = f;
    g_ = g;
  }
  virtual ~ClassCallBack1_4() {}
  virtual void RunCallback(H h) { (c_->*cb_)(d_, e_, f_, g_, h); }

 private:
  void (C::*cb_)(D, E, F, G, H);
  C* c_;
  D d_;
  E e_;
  F f_;
  G g_;
};

//
// Class CallBack1 specific functions
//

template <typename C, typename D>
CallBack1<D>* CreateCallback(C* c, void (C::*cb)(D)) {
  auto cb0 = new ClassCallBack1_0<C, D>(false, c, cb);
  return cb0;
}

template <typename C, typename D, typename E>
CallBack1<E>* CreateCallback(C* c, void (C::*cb)(D, E), D d) {
  auto cb1 = new ClassCallBack1_1<C, D, E>(false, c, cb, d);
  return cb1;
}

template <typename C, typename D, typename E, typename F>
CallBack1<F>* CreateCallback(C* c, void (C::*cb)(D, E, F), D d, E e) {
  auto cb2 = new ClassCallBack1_2<C, D, E, F>(false, c, cb, d, e);
  return cb2;
}

template <typename C, typename D, typename E, typename F, typename G>
CallBack1<G>* CreateCallback(C* c, void (C::*cb)(D, E, F, G), D d, E e, F f) {
  auto cb3 = new ClassCallBack1_3<C, D, E, F, G>(false, c, cb, d, e, f);
  return cb3;
}

template <typename C, typename D, typename E, typename F, typename G, typename H>
CallBack1<H>* CreateCallback(C* c, void (C::*cb)(D, E, F, G, H), D d, E e, F f, G g) {
  auto cb4 = new ClassCallBack1_4<C, D, E, F, G, H>(false, c, cb, d, e, f, g);
  return cb4;
}

template <typename C, typename D>
CallBack1<D>* CreatePersistentCallback(C* c, void (C::*cb)(D)) {
  auto cb0 = new ClassCallBack1_0<C, D>(true, c, cb);
  return cb0;
}

template <typename C, typename D, typename E>
CallBack1<E>* CreatePersistentCallback(C* c, void (C::*cb)(D, E), D d) {
  auto cb1 = new ClassCallBack1_1<C, D, E>(true, c, cb, d);
  return cb1;
}

template <typename C, typename D, typename E, typename F>
CallBack1<F>* CreatePersistentCallback(C* c, void (C::*cb)(D, E, F), D d, E e) {
  auto cb2 = new ClassCallBack1_2<C, D, E, F>(true, c, cb, d, e);
  return cb2;
}

template <typename C, typename D, typename E, typename F, typename G>
CallBack1<G>* CreatePersistentCallback(C* c, void (C::*cb)(D, E, F, G), D d, E e, F f) {
  auto cb3 = new ClassCallBack1_3<C, D, E, F, G>(true, c, cb, d, e, f);
  return cb3;
}

template <typename C, typename D, typename E, typename F, typename G, typename H>
CallBack1<H>* CreatePersistentCallback(C* c, void (C::*cb)(D, E, F, G, H), D d, E e, F f, G g) {
  auto cb4 = new ClassCallBack1_4<C, D, E, F, G, H>(true, c, cb, d, e, f, g);
  return cb4;
}

#endif  // HERON_CLASS_CALLBACK1_H_
