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

#if !defined(__SP_TIME_H)
#define __SP_TIME_H

#include <iostream>
#include "config/heron-config.h"
#include "basics/sptypes.h"

#ifndef _WINDOWS
extern "C" {
#include <sys/time.h>
}
#endif

#if defined(IS_MACOSX)
#undef USE_POSIX_TIME
#else
#define USE_POSIX_TIME
#endif

#define NS_SECOND 1000000000 /* nanoseconds in a second */
#define US_SECOND 1000000    /* microseconds in a second */
#define MS_SECOND 1000       /* millisecs in a second */

class sp_time {
 public:
  enum Unit { NOTIMEUNIT, SECOND, MINUTE, HOUR, DAY };

 protected:
#ifdef USE_POSIX_TIME
  struct timespec time_;
#else
  struct timeval time_;
#endif

  /* better method name, PLEASE */
  void gettime();

  void signs();
  void _normalize();
  void normalize();

  /* only good INSIDE, since workings exposed if public */
  sp_time(time_t, sp_int64); /* time-of-day, hr-secs */

 public:
  sp_time() { reset(); }
  void reset() {
    time_.tv_sec = 0;
#ifdef USE_POSIX_TIME
    time_.tv_nsec = 0;
#else
    time_.tv_usec = 0;
#endif
  }

  explicit sp_time(const struct timeval &tv);

#ifdef USE_POSIX_TIME
  explicit sp_time(const struct timespec &ts);
#endif

  // an interval in seconds.
  explicit sp_time(sp_int32);
  explicit sp_time(sp_int64);

  // an interval in floating point seconds
  explicit sp_time(sp_double64);

  /* comparison primitives */
  bool operator==(const sp_time &) const;
  bool operator<(const sp_time &) const;
  bool operator<=(const sp_time &) const;

  /* derived compares */
  bool operator!=(const sp_time &r) const { return !(*this == r); }
  bool operator>(const sp_time &r) const { return !(*this <= r); }
  bool operator>=(const sp_time &r) const { return !(*this < r); }

  // negate an interval
  sp_time operator-() const;

  /* times can be added and subtracted */
  sp_time operator+(const sp_time &r) const;
  sp_time operator-(const sp_time &r) const;

  /* adjust an interval by a factor ... an experiment in progress */
  /* XXX should this be confined to Time_Interval ??? */
  sp_time operator*(const sp_int32 factor) const;
  sp_time operator/(const sp_int32 factor) const;
  sp_time operator*(const sp_double64 factor) const;
  sp_time operator/(const sp_double64 factor) const;

  // operatorX= variants
  sp_time &operator+=(const sp_time &r);
  sp_time &operator-=(const sp_time &r);

  /* output conversions */
  operator sp_double64() const;
  operator sp_double32() const;
  operator struct timeval() const;
  operator struct timespec() const;

  /* XXX really belongs in Time_Interval */
  /* simple output conversions for integers to eliminate fp */
  sp_int64 secs() const;  /* seconds */
  sp_int64 msecs() const; /* milli seconds */
  sp_int64 usecs() const; /* micro seconds */
  sp_int64 nsecs() const; /* nano seconds */

  /* input conversion operators for integral types */
  static sp_time sec(sp_int32 seconds);
  static sp_time usec(sp_int32 micro_seconds, sp_int32 seconds = 0);
  static sp_time msec(sp_int32 milli_seconds, sp_int32 seconds = 0);
  static sp_time nsec(sp_int32 nano_seconds, sp_int32 seconds = 0);

  /* the Current time */
  static sp_time now();

  /* get the time from now */
  static sp_time range(sp_int32 len, Unit unit);

  std::ostream &print(std::ostream &s) const;
  std::ostream &ctime_r(std::ostream &s) const;
};

/*
 * Intervals are different, in some ways, from absolute times. For now,
 * they are to change print methods.
 */

class sp_time_interval : public sp_time {
 public:
  /* XXX why do I duplicate the constructors ???  There
   * is or was a reason for it. */

  sp_time_interval() : sp_time() {}
  explicit sp_time_interval(const struct timeval &tv) : sp_time(tv) {}

#ifdef USE_POSIX_TIME
  explicit sp_time_interval(const struct timespec &ts) : sp_time(ts) {}
#endif

  explicit sp_time_interval(const sp_time &time) : sp_time(time) {}
  explicit sp_time_interval(sp_int32 time) : sp_time(time) {}
  explicit sp_time_interval(sp_int64 time) : sp_time(time) {}
  explicit sp_time_interval(sp_double64 time) : sp_time(time) {}

  std::ostream &print(std::ostream &s) const;
};

extern std::ostream &operator<<(std::ostream &s, const sp_time &t);
extern std::ostream &operator<<(std::ostream &s, const sp_time_interval &t);

#endif
