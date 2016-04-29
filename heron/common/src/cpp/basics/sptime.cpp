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

#include "basics/sptime.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

/*
 * All this magic is to allow either timevals or timespecs
 * to be used without code change.  It's disgusting, but it
 * avoid templates.  Templates are evil.

 * st_tod	== time of day part of the time
 * st_hires	== "higher resolution" part of the time
 * HR_SECOND	== high-resolution units in a second
 */

#if defined(USE_POSIX_TIME)

typedef struct timespec _sp_time_t;
#define st_tod tv_sec
#define st_hires tv_nsec
#define HR_SECOND NS_SECOND

#else

typedef struct timeval _sp_time_t;
#define st_tod tv_sec
#define st_hires tv_usec
#define HR_SECOND US_SECOND

#endif

#if !defined(IS_MACOSX)
extern "C" int gettimeofday(struct timeval *, struct timezone *);
#endif

/* Internal constructor, exposes implementation */
sp_time::sp_time(time_t tod, sp_int64 hires) {
  time_.st_tod = tod;
  time_.st_hires = hires;

  normalize();
}

sp_time::sp_time(sp_int32 secs) {
  time_.st_tod = secs;
  time_.st_hires = 0;

  /* the conversion automagically normalizes */
}

sp_time::sp_time(sp_int64 secs) {
  time_.st_tod = secs;
  time_.st_hires = 0;

  /* the conversion automagically normalizes */
}

sp_time::sp_time(sp_double64 secs) {
  time_.st_tod = (sp_int64)secs;
  time_.st_hires = (sp_int64)((secs - time_.st_tod) * HR_SECOND);

  /* the conversion automagically normalizes */
}

sp_time::sp_time(const struct timeval &tv) {
  time_.st_tod = tv.tv_sec;
  time_.st_hires = tv.tv_usec * (HR_SECOND / US_SECOND);

  normalize();
}

#if defined(USE_POSIX_TIME)
sp_time::sp_time(const struct timespec &tv) {
  time_.st_tod = tv.tv_sec;
  time_.st_hires = tv.tv_nsec * (HR_SECOND / NS_SECOND);

  normalize();
}
#endif

bool sp_time::operator==(const sp_time &r) const {
  return time_.st_tod == r.time_.st_tod && time_.st_hires == r.time_.st_hires;
}

bool sp_time::operator<(const sp_time &r) const {
  if (time_.st_tod == r.time_.st_tod) return time_.st_hires < r.time_.st_hires;

  return time_.st_tod < r.time_.st_tod;
}

bool sp_time::operator<=(const sp_time &r) const { return *this == r || *this < r; }

static inline sp_int32 sign(const sp_int32 i) { return i > 0 ? 1 : i < 0 ? -1 : 0; }

/* Put a stime into normal form, where the HIRES part
   will contain less than a TODs worth of HIRES time.
   Also, the signs of the TOD and HIRES parts should
   agree (unless TOD==0) */

void sp_time::signs() {
  if (time_.st_tod && time_.st_hires && sign(time_.st_tod) != sign(time_.st_hires)) {
    if (sign(time_.st_tod) == 1) {
      time_.st_tod--;
      time_.st_hires += HR_SECOND;
    } else {
      time_.st_tod++;
      time_.st_hires -= HR_SECOND;
    }
  }
}

/* off-by one */
void sp_time::_normalize() {
  if (abs(time_.st_hires) >= HR_SECOND) {
    time_.st_tod += sign(time_.st_hires);
    time_.st_hires -= sign(time_.st_hires) * HR_SECOND;
  }

  signs();
}

/* something that could be completely wacked out */
void sp_time::normalize() {
  sp_int32 factor;

  factor = time_.st_hires / HR_SECOND;
  if (factor) {
    time_.st_tod += factor;
    time_.st_hires -= HR_SECOND * factor;
  }

  signs();
}

sp_time sp_time::operator-() const {
  sp_time result;

  result.time_.st_tod = -time_.st_tod;
  result.time_.st_hires = -time_.st_hires;

  return result;
}

sp_time sp_time::operator+(const sp_time &r) const {
  sp_time result;

  result.time_.st_tod = time_.st_tod + r.time_.st_tod;
  result.time_.st_hires = time_.st_hires + r.time_.st_hires;

  result._normalize();

  return result;
}

sp_time sp_time::operator-(const sp_time &r) const { return *this + -r; }

sp_time sp_time::operator*(const sp_int32 factor) const {
  sp_time result;

  result.time_.st_tod = time_.st_tod * factor;
  result.time_.st_hires = time_.st_hires * factor;
  result.normalize();

  return result;
}

/* XXX
   Float scaling is stupid for the moment.  It doesn't need
   to use sp_double64 arithmetic, instead it should use an
   intermediate normalization step which moves
   lost TOD units into the HIRES range.

   The sp_double64 stuff at least makes it seem to work right.
 */

sp_time sp_time::operator/(const sp_int32 factor) const { return *this / (sp_double64)factor; }

sp_time sp_time::operator*(const sp_double64 factor) const {
  sp_double64 d = *this;
  d *= factor;
  sp_time result(d);
  result.normalize();

  return result;
}

sp_time sp_time::operator/(const sp_double64 factor) const { return *this * (1.0 / factor); }

/* The operator X and operator X= can be written in terms of each other */
sp_time &sp_time::operator+=(const sp_time &r) {
  time_.st_tod += r.time_.st_tod;
  time_.st_hires += r.time_.st_hires;

  _normalize();

  return *this;
}

sp_time &sp_time::operator-=(const sp_time &r) {
  time_.st_tod -= r.time_.st_tod;
  time_.st_hires -= r.time_.st_hires;

  _normalize();

  return *this;
}

sp_time::operator sp_double64() const {
  return time_.st_tod + time_.st_hires / (sp_double64)HR_SECOND;
}

sp_time::operator sp_double32() const {
  sp_double64 res = (sp_double64) * this;
  return (sp_double32)res;
}

sp_time::operator struct timeval() const {
  struct timeval tv;
  tv.tv_sec = time_.st_tod;

  /* This conversion may prevent overflow which may
     occurs with some values on some systems. */

  tv.tv_usec = time_.st_hires / (HR_SECOND / US_SECOND);
  return tv;
}

/* XXX do we want this conversion even if we are using timeval
   implementation on systems that have timespec? */
sp_time::operator struct timespec() const {
  struct timespec tv;
  tv.tv_sec = time_.st_tod;
  tv.tv_nsec = time_.st_hires;
  return tv;
}

void sp_time::gettime() {
  sp_int32 kr;
#if defined(USE_POSIX_TIME)
  kr = clock_gettime(CLOCK_REALTIME, &time_);
#else
  kr = gettimeofday(&time_, 0);
#endif
  if (kr == -1) {
    abort();  // TO DO: Revamp to use the new error handling mechanism
  }
}

std::ostream &sp_time::print(std::ostream &s) const {
  ctime_r(s);

  if (time_.st_hires) {
    sp_time tod(time_.st_tod, 0);

    s << " and " << sp_time_interval(*this - tod);
  }

  return s;
}

std::ostream &sp_time::ctime_r(std::ostream &s) const {
  struct tm *local;
  char *when;
  char *nl;

  /* the second field of the time structs should be a time_t */
  time_t kludge = time_.st_tod;

  /* XXX use a reentrant form if available */
  local = localtime(&kludge);
  when = asctime(local);

  /* chop the newline */
  nl = strchr(when, '\n');
  if (nl) *nl = '\0';

  return s << when;
}

static void factor_print(std::ostream &s, sp_int64 what) {
  struct {
    const char *label;
    sp_int32 factor;
  } factors[] = {{"%02d:", 60 * 60}, {"%02d:", 60}, {0, 0}}, *f = factors;

  sp_int64 mine;
  bool printed = false;
  char print_str[256];

  for (f = factors; f->label; f++) {
    mine = what / f->factor;
    what = what % f->factor;
    if (mine || printed) {
      snprintf(print_str, sizeof(print_str), f->label, mine);
      s << print_str;
      printed = true;
    }
  }

  /* always print a seconds field */
  snprintf(print_str, sizeof(print_str), printed ? "%02lld" : "%lld", what);
  s << print_str;
}

std::ostream &sp_time_interval::print(std::ostream &s) const {
  char print_str[256];

  /* XXX should decode interval in hours, min, sec, usec, nsec */
  factor_print(s, time_.st_tod);

  /* XXX should print ds_int16 versions, aka .375 etc */
  if (time_.st_hires) {
#if defined(USE_POSIX_TIME)
    snprintf(print_str, sizeof(print_str), ".%09ld", time_.st_hires);
#else
    snprintf(print_str, sizeof(print_str), ".%06d", time_.st_hires);
#endif

    s << print_str;
  }

  return s;
}

std::ostream &operator<<(std::ostream &s, const sp_time &t) { return t.print(s); }

std::ostream &operator<<(std::ostream &s, const sp_time_interval &t) { return t.print(s); }

/* Input Conversion operators */

static inline void from_linear(sp_int32 sec, sp_int32 xsec, sp_int32 linear_secs,
                               _sp_time_t &time_) {
  time_.st_tod = sec + xsec / linear_secs;
  xsec = xsec % linear_secs;
  if (linear_secs > HR_SECOND)
    time_.st_hires = xsec / (linear_secs / HR_SECOND);
  else
    time_.st_hires = xsec * (HR_SECOND / linear_secs);
}

sp_time sp_time::sec(sp_int32 sec) {
  sp_time r;

  r.time_.st_tod = sec;
  r.time_.st_hires = 0;

  return r;
}

sp_time sp_time::msec(sp_int32 ms, sp_int32 sec) {
  sp_time r;

  from_linear(sec, ms, MS_SECOND, r.time_);

  return r;
}

sp_time sp_time::usec(sp_int32 us, sp_int32 sec) {
  sp_time r;

  from_linear(sec, us, US_SECOND, r.time_);

  return r;
}

sp_time sp_time::nsec(sp_int32 ns, sp_int32 sec) {
  sp_time r;

  from_linear(sec, ns, NS_SECOND, r.time_);
  /* conversion normalizes */

  return r;
}

sp_time sp_time::now() {
  sp_time now;
  now.gettime();

  return now;
}

sp_time sp_time::range(sp_int32 len, Unit unit) {
  switch (unit) {
    case SECOND:
      return sp_time(len);

    case MINUTE:
      return sp_time(len * 60);

    case HOUR:
      return sp_time(len * 60 * 60);

    case DAY:
      return sp_time(len * 24 * 60 * 60);

    default:
      break;
  }

  return sp_time();
}

/* More conversion operators */
/* For now, only the seconds conversion does rounding */

/* roundup #seconds if hr_seconds >= this value */
#define HR_ROUNDUP (HR_SECOND / 2)

static inline sp_int64 to_linear(const _sp_time_t &time_, const sp_int32 linear_secs) {
  sp_int64 result;
  sp_int32 factor;

  result = time_.st_tod * linear_secs;

  if (linear_secs > HR_SECOND) {
    factor = linear_secs / HR_SECOND;
    result += time_.st_hires * factor;
  } else {
    factor = HR_SECOND / linear_secs;
    result += time_.st_hires / factor;
  }

  return result;
}

sp_int64 sp_time::secs() const {
  sp_int64 result;

  result = time_.st_tod;
  if (time_.st_hires >= HR_ROUNDUP) result++;

  return result;
}

sp_int64 sp_time::msecs() const { return to_linear(time_, MS_SECOND); }

sp_int64 sp_time::usecs() const { return to_linear(time_, US_SECOND); }

sp_int64 sp_time::nsecs() const { return to_linear(time_, NS_SECOND); }
