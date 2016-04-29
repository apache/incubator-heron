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

#ifndef HERON_CALLBACK1_H_
#define HERON_CALLBACK1_H_

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
// CallBack1<int> cb = CreateCallback(void (*func)(char, int), 'c');
// Routine(arg, arg2, cb);

template <typename C>
class CallBack1 {
 public:
  // Constructor. Users should use the CreateCallback functions detailed below.
  explicit CallBack1(bool persist) { persistent_ = persist; }

  // Virtual destructor.
  virtual ~CallBack1() {}

  // The main exported function
  void Run(C c) {
    // In case of persistent callbacks, the RunCallback might delete itself.
    // In that case after RunCallback returns it is not safe to access
    // the class member persistent_. So make a copy now.
    bool persist = persistent_;
    RunCallback(c);
    if (!persist) {
      delete this;
    }
  }

  // Return whether the callback is temporary or permanent.
  bool isPersistent() const { return persistent_; }

 protected:
  virtual void RunCallback(C) = 0;

 private:
  bool persistent_;
};

/*
 * Defines CallBack1 for functions that take no arguments
 */
template <typename C>
class CallBack1_0 : public CallBack1<C> {
 public:
  CallBack1_0(bool persist, void (*cb)(C)) : CallBack1<C>(persist) { cb_ = cb; }
  virtual ~CallBack1_0() {}
  virtual void RunCallback(C c) { cb_(c); }

 private:
  void (*cb_)(C);
};

/*
 * Defines CallBack1 for functions that take 1 argument
 */
template <typename C, typename D>
class CallBack1_1 : public CallBack1<D> {
 public:
  CallBack1_1(bool persist, void (*cb)(C, D), C c) : CallBack1<D>(persist) {
    cb_ = cb;
    c_ = c;
  }
  virtual ~CallBack1_1() {}
  virtual void RunCallback(D d) { cb_(c_, d); }

 private:
  void (*cb_)(C, D);
  C c_;
};

/*
 * Defines CallBack1 for functions that take 2 arguments
 */
template <typename C, typename D, typename E>
class CallBack1_2 : public CallBack1<E> {
 public:
  CallBack1_2(bool persist, void (*cb)(C, D, E), C c, D d) : CallBack1<E>(persist) {
    cb_ = cb;
    c_ = c;
    d_ = d;
  }
  virtual ~CallBack1_2() {}
  virtual void RunCallback(E e) { cb_(c_, d_, e); }

 private:
  void (*cb_)(C, D, E);
  C c_;
  D d_;
};

/*
 * Defines CallBack1 for functions that take 3 arguments
 */
template <typename C, typename D, typename E, typename F>
class CallBack1_3 : public CallBack1<F> {
 public:
  CallBack1_3(bool persist, void (*cb)(C, D, E, F), C c, D d, E e) : CallBack1<F>(persist) {
    cb_ = cb;
    c_ = c;
    d_ = d;
    e_ = e;
  }
  virtual ~CallBack1_3() {}
  virtual void RunCallback(F f) { cb_(c_, d_, e_, f); }

 private:
  void (*cb_)(C, D, E, F);
  C c_;
  D d_;
  E e_;
};

//
// CallBack1 specific functions
//

template <typename C>
CallBack1<C>* CreateCallback(void (*cb)(C)) {
  auto cb0 = new CallBack1_0<C>(false, cb);
  return cb0;
}

template <typename C, typename D>
CallBack1<D>* CreateCallback(void (*cb)(C, D), C c) {
  auto cb1 = new CallBack1_1<C, D>(false, cb, c);
  return cb1;
}

template <typename C, typename D, typename E>
CallBack1<E>* CreateCallback(void (*cb)(C, D, E), C c, D d) {
  auto cb2 = new CallBack1_2<C, D, E>(false, cb, c, d);
  return cb2;
}

template <typename C, typename D, typename E, typename F>
CallBack1<F>* CreateCallback(void (*cb)(C, D, E, F), C c, D d, E e) {
  auto cb3 = new CallBack1_3<C, D, E, F>(false, cb, c, d, e);
  return cb3;
}

template <typename C>
CallBack1<C>* CreatePersistentCallback(void (*cb)(C)) {
  auto cb0 = new CallBack1_0<C>(true, cb);
  return cb0;
}

template <typename C, typename D>
CallBack1<D>* CreatePersistentCallback(void (*cb)(C, D), C c) {
  auto cb1 = new CallBack1_1<C, D>(true, cb, c);
  return cb1;
}

template <typename C, typename D, typename E>
CallBack1<E>* CreatePersistentCallback(void (*cb)(C, D, E), C c, D d) {
  auto cb2 = new CallBack1_2<C, D, E>(true, cb, c, d);
  return cb2;
}

template <typename C, typename D, typename E, typename F>
CallBack1<F>* CreatePersistentCallback(void (*cb)(C, D, E, F), C c, D d, E e) {
  auto cb3 = new CallBack1_3<C, D, E, F>(true, cb, c, d, e);
  return cb3;
}

#endif  // HERON_CLASS_CALLBACK1_H_
