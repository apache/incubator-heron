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

/**
 * Basic CallBack definition.
 * There are two types of CallBacks.
 *   - Temporary callback - the CallBack gets deleted as soon as its invoked.
 *   - Persistent callBack - the CallBack remains across multiple invocations.
 */
class CallBack {
 public:
  // Constructor - Users should not use this directly. Instead, use the
  // CreateCallback functions detailed later in this file for creating CallBacks.
  explicit CallBack(bool persist) {
    persistent_ = persist;
  }

  // Virtual Destructor
  virtual ~CallBack() { }

  // The main exported method. This invokes the underlying function and cleans up
  // if necessary.
  void Run() {
    // In case of permanent callbacks, the RunCallback might delete itself.
    // For such cases after RunCallback returns it is not safe to access
    // the class member persistent_. So make a copy now.
    bool persist = persistent_;
    RunCallback();
    if (!persist) { delete this; }
  }

  // Returns if the CallBack is a temporary or permanent callback.
  bool isPersistent() const {
    return persistent_;
  }

  void makePersistent() {
    persistent_ = true;
  }

 protected:
  // See callback.cc for details.
  virtual void RunCallback() = 0;

 private:
  bool persistent_;
};

/*
 * Defines a callback for functions that take no arguments
 */
class CallBack_0 : public CallBack {
 public:
  CallBack_0(bool persist, void (*cb)()) : CallBack(persist) {
    cb_ = cb;
  }

  virtual ~CallBack_0() { }

  virtual void RunCallback() {
    cb_();
  }

 private:
  void (*cb_)();                    // function ptr with no arg
};

/*
 * Defines a callback for functions that take 1 arg
 */
template<typename C>
class CallBack_1 : public CallBack {
 public:
  CallBack_1(bool persist, void(*cb)(C), C c) : CallBack(persist) {
    cb_ = cb;
    c_ = c;
  }
  virtual ~CallBack_1() { }
  virtual void RunCallback() {
    cb_(c_);
  }
 private:
  void  (*cb_)(C);                  // function ptr with 1 arg
  C c_;                             // actual value of the arg
};

/*
 * Defines call for functions that take 2 args
 */
template<typename C, typename D>
class CallBack_2 : public CallBack {
 public:
  CallBack_2(bool _permanent, void(*cb)(C, D), C c, D d) :
    CallBack(_permanent) {
    cb_ = cb;
    c_ = c;
    d_ = d;
  }
  virtual ~CallBack_2() { }
  virtual void RunCallback() {
    cb_(c_, d_);
  }
 private:
  void (*cb_)(C, D);                // function ptr with 2 args
  C c_;                             // actual value of the 1st arg
  D d_;                             // actual value of the 2nd arg
};

/*
 * Defines callback for functions that take 3 args
 */
template<typename C, typename D, typename E>
class CallBack_3 : public CallBack {
 public:
  CallBack_3(bool _permanent, void(*cb)(C, D, E), C c, D d, E e)
    : CallBack(_permanent) {
    cb_ = cb;
    c_ = c;
    d_ = d;
    e_ = e;
  }
  virtual ~CallBack_3() { }
  virtual void RunCallback() {
    cb_(c_, d_, e_);
  }
 private:
  void (*cb_)(C, D, E);             // function ptr with 3 args
  C c_;                             // actual value of the 1st arg
  D d_;                             // actual value of the 2nd arg
  E e_;                             // actual value of the 3rd arg
};

//
// Helper functions to create callbacks
//
inline
CallBack* CreateCallback(void (*cb)()) {
  auto cb0 = new CallBack_0(false, cb);
  return cb0;
}

template<typename C>
CallBack* CreateCallback(void (*cb)(C), C c) {
  auto cb1 = new CallBack_1<C>(false, cb, c);
  return cb1;
}

template<typename C, typename D>
CallBack* CreateCallback(void (*cb)(C, D), C c, D d) {
  auto cb2 = new CallBack_2<C, D>(false, cb, c, d);
  return cb2;
}

template<typename C, typename D, typename E>
CallBack* CreateCallback(void (*cb)(C, D, E), C c, D d, E e) {
  auto cb3 = new CallBack_3<C, D, E>(false, cb, c, d, e);
  return cb3;
}

inline
CallBack* CreatePersistentCallback(void (*cb)()) {
  auto cb0 = new CallBack_0(true, cb);
  return cb0;
}

template<typename C>
CallBack* CreatePersistentCallback(void (*cb)(C), C c) {
  auto cb1 = new CallBack_1<C>(true, cb, c);
  return cb1;
}

template<typename C, typename D>
CallBack* CreatePersistentCallback(void (*cb)(C, D), C c, D d) {
  auto cb2 = new CallBack_2<C, D>(true, cb, c, d);
  return cb2;
}

template<typename C, typename D, typename E>
CallBack* CreatePersistentCallback(void (*cb)(C, D, E), C c, D d, E e) {
  auto cb3 = new CallBack_3<C, D, E>(true, cb, c, d, e);
  return cb3;
}

#endif  // HERON_CALLBACK_H_
