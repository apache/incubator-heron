#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)

AC_INIT([heron], m4_esyscmd([if [ "${HERON_VERSION-x}" = "x" ]; then echo "unversioned"; else echo $HERON_VERSION; fi | tr -d '\n']))

# Call any config specific stuff

AC_CONFIG_SRCDIR([heron.def])
AC_CONFIG_HEADERS([heron-config.h:heron-config-h.in])
AC_CONFIG_MACRO_DIR([m4])

AC_COPYRIGHT(COPYRIGHT-NOTICE)
AC_CONFIG_AUX_DIR(config)

# autoconf's default CXXFLAGS are usually "-g -O2".  These aren't necessarily
# the best choice for libprotobuf.
AS_IF([test "x${ac_cv_env_CFLAGS_set}" = "x"],
      [CFLAGS=""])
AS_IF([test "x${ac_cv_env_CXXFLAGS_set}" = "x"],
      [CXXFLAGS=""])

# Have autoconf setup some variables related to the system
AC_CANONICAL_TARGET

# Initialize automake
AM_INIT_AUTOMAKE([subdir-objects -Wall -Werror -Wno-portability])

# Check for existence of programs.
AC_PROG_CXXCPP
AC_PROG_LIBTOOL
AC_REQUIRE_CPP
AC_PROG_INSTALL
AC_CHECK_TOOL(RMM, rm, /bin/rm)

# Checks for gcc toolchain (we rely on some atomic builtins for now).
AC_PROG_CC([gcc])
AC_PROG_CXX([g++])

# Check the version of gcc and add any flags as appropriate.
GCC_VERSION="`gcc -dumpversion`"
test $? = 0 || AC_MSG_ERROR([failed to determine version of gcc])
case  "${GCC_VERSION}" in
  4.4*)
    echo ===========================================================
    echo Setting up CXXFLAGS for gcc 4.4.*
    echo ===========================================================
    # We fail to build some protobuf generated code with gcc 4.4
    # without setting -fno-strict-aliasing.
    CFLAGS="$CFLAGS -fno-strict-aliasing"
    CXXFLAGS="$CXXFLAGS -fno-strict-aliasing"
  ;;
esac

# Get the user and host info
username=$(whoami)
AC_MSG_CHECKING([user ID])
AC_MSG_RESULT([$username])

hostname=$(uname -n)
AC_MSG_CHECKING([host name])
AC_MSG_RESULT([$hostname])

timestamp=$(date)
AC_MSG_CHECKING([current timestamp])
AC_MSG_RESULT([$timestamp])

AC_DEFINE_UNQUOTED([PACKAGE_COMPILE_USER], "$username", [user compiled])
AC_DEFINE_UNQUOTED([PACKAGE_COMPILE_HOST], "$hostname", [host of compiled])
AC_DEFINE_UNQUOTED([PACKAGE_COMPILE_TIME], "$timestamp",[time of compilation])

# Check if we compiling for Mac or Linux
case $host_os in
 darwin*) sys_os=darwin ;;
 linux*) sys_os=linux ;;
 *) AC_MSG_ERROR([Your platform is not currently supported]) ;;
esac
AM_CONDITIONAL([RTLIBS], [test x$sys_os = xlinux])
AM_CONDITIONAL([SYSLINUX], [test x$sys_os = xlinux])

# Check if we have pthread
ACX_PTHREAD

# Check the python version required
AM_PATH_PYTHON([3.4])
AC_PATH_PROG([PYTHON3], [python3], [],[])

abs_top_builddir=`pwd`
AC_SUBST(abs_top_builddir)

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_C_INLINE
AC_TYPE_SIZE_T
AC_HEADER_TIME
AC_CHECK_TYPES([ptrdiff_t])

AH_TEMPLATE(I386,	[architecture])
AH_TEMPLATE(i386,	[architecture - not always defined by compiler])
AH_TEMPLATE(ARCH_LP64,	[64 bit architecture])

AH_TEMPLATE(Linux,	[operating system])
AH_TEMPLATE(MacOSX,	[operating system])
AH_TEMPLATE(__MacOSX__,	[operating system])

AH_TEMPLATE(IS_LINUX,	[operating system])
AH_TEMPLATE(IS_MACOSX,	[operating system])
AH_TEMPLATE(win32,	[operating system])

AH_TEMPLATE(IS_I386_CYGWIN,	[combo])
AH_TEMPLATE(IS_I386_LINUX,	[combo])
AH_TEMPLATE(IS_I386_MACOSX,	[combo])
AH_TEMPLATE(IS_IX86_NT,		[combo])
AH_TEMPLATE(IS_IX86_NT,		[combo])
AH_TEMPLATE(IS_I386_CYGWIN,	[combo])

AH_TEMPLATE(IS_UNIX,		[windows or unix?])
AH_TEMPLATE(IS_NT,		[opposite of IS_UNIX])
AH_TEMPLATE(IS_DOSDEV,		[dos-based])
AH_TEMPLATE(IS_CYGWIN,		[windows])

## ---------------------------------------------------------------
##
## -------------- Architecture
##
## NOTE: all these IS_xxx, including IS_NT and IS_UNIX
## are not used in the sources and can be removed. For now, they
## are left in for developers' convenience.
##
## ---------------------------------------------------------------

AC_LANG_PUSH(C++)

AC_MSG_CHECKING([architecture])
AC_PREPROC_IFELSE([AC_LANG_PROGRAM(
[[#if defined(_WIN32) && defined(_M_IX86) && defined(_MSC_VER)
#else
#error not this architecture
#endif
]])],
[AC_DEFINE(I386,,[])
AC_DEFINE(i386)
AC_DEFINE(win32,,[])
AC_DEFINE(IS_IX86_NT,1,[x86 nt])
AC_DEFINE(IS_NT,1,[is nt])
AC_DEFINE(IS_DOSDEV,1,[dos-based])
AC_MSG_RESULT([dos-based nt i386])
],
[])

AC_PREPROC_IFELSE([AC_LANG_PROGRAM(
[[#if defined(_WIN32) && defined(__GNUC__) && defined(i386)
#else
#error not this architecture
#endif
]])],
[AC_DEFINE(I386,,[])
AC_DEFINE(win32,,[])
AC_DEFINE(IS_NT,1,[is nt])
AC_DEFINE(IS_CYGWIN,1,[nt cygwin])
AC_DEFINE(IS_I386_CYGWIN,1,[x86 nt cygwin])
AC_MSG_RESULT([cygwin nt i386])
],
[])

AC_PREPROC_IFELSE([AC_LANG_PROGRAM(
[[#if defined(linux) && defined(i386)
#else
#error not this architecture
#endif
]])],
[AC_DEFINE(I386,,[])
AC_DEFINE(i386)
AC_DEFINE(Linux,,[])
AC_DEFINE(IS_I386_LINUX,1,[i386 linux])
AC_DEFINE(IS_LINUX,1,[linux])
AC_MSG_RESULT([linux i386])
],
[])

AC_PREPROC_IFELSE([AC_LANG_PROGRAM(
[[#if defined(linux) && defined(__x86_64)
#else
#error not this architecture
#endif
]])],
[AC_DEFINE(I386,,[])
AC_DEFINE(i386)
AC_DEFINE(Linux,,[])
AC_DEFINE(IS_I386_LINUX,1,[i386 linux])
AC_DEFINE(IS_LINUX,1,[linux])
AC_DEFINE(ARCH_LP64,1,[for amd64])
AC_MSG_RESULT([linux 64 bit])
],
[])

AC_PREPROC_IFELSE([AC_LANG_PROGRAM(
[[#if defined(__x86_64) && defined(__APPLE__)
#else
#error not this architecture
#endif
]])],
[AC_DEFINE(I386,,[])
AC_DEFINE(MacOSX,,[])
AC_DEFINE(__MacOSX__)
AC_DEFINE(IS_I386_MACOSX,1,[i386 macosx])
AC_DEFINE(IS_MACOSX,1,[apple mac osx])
AC_MSG_RESULT([mac osx i386])
],
[])

## -------------- IS NT OR UNIX

AH_VERBATIM([isunix],[/*simply !IS_NT*/
#ifndef IS_NT
#define IS_UNIX 1
#endif
])
AC_LANG_POP(C++)

# -------------------------------------------------------------------------
#
# handle --enable-debug
# Needed at config time to tell make what flags to put on the compiles,
# and what symbols, -DDEBUG -UNDEBUG
# We need to put the macro definitions into the shore-config.h
# and take them out of the compile command line.
#
# enabling this feature is equiv to DEBUGCODE ON
#
# -------------------------------------------------------------------------

AC_ARG_ENABLE([debug],
  AS_HELP_STRING([--enable-debug], [Turn on full debugging: symbols & code [default=no]]),
  [debug=$enableval],
  [debug=no])
AM_CONDITIONAL([ENABLE_DEBUG], [test x$debug = xyes])

# -------------------------------------------------------------------------
#
# handle --enable-coverage
#
# -------------------------------------------------------------------------
AC_ARG_ENABLE([coverage],
  AS_HELP_STRING([--enable-coverage], [Turn on coverage reporting with lcov [default=no]]),
  [enable_coverage=$enableval],
  [enable_coverage=no])
AM_CONDITIONAL([ENABLE_COVERAGE], [test x$enable_coverage = xyes])


# Save the configure arguments so we can pass them to any third-party
# libraries that we might run configure on (see
# third_party/Makefile.am). One downside of our strategy for shipping
# and building third-party libraries is that we can't expose options
# from nested third-party configure scripts.
CONFIGURE_ARGS="$ac_configure_args"
AC_SUBST(CONFIGURE_ARGS)
AC_CONFIG_FILES([Makefile])

AC_OUTPUT

AC_MSG_RESULT([
Configure Information:
  C Compiler        :   $CC
    DEFS            :   $DEFS
    CPPFLAGS        :   $CPPFLAGS
    CFLAGS          :   $CFLAGS

  C++ Compiler      :   $CXX
    DEFS            :   $DEFS
    CPPFLAGS        :   $CPPFLAGS
    CXXFLAGS        :   $CXXFLAGS

  Linker            :   $LD
    LDFLAGS         :   $LDFLAGS
    LIBS            :   $LIBS

  Python            :   $PYTHON
])
