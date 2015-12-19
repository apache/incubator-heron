AC_DEFUN([AC_CXX_NEW_GNUCXX_STDIO_FILEBUF],[
AC_CACHE_CHECK(for the 2-argument constructor of __gnu_cxx::stdio_filebuf,
ac_cv_cxx_new_gnucxx_stdio_filebuf,
[AC_LANG_PUSH([C++])
 AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <ext/stdio_filebuf.h>
#include <istream>
int fd = 0;
__gnu_cxx::stdio_filebuf<char> *fb = new __gnu_cxx::stdio_filebuf<char>(fd,std::ios_base::in);
]])],
 ac_cv_cxx_new_gnucxx_stdio_filebuf=yes,
 ac_cv_cxx_new_gnucxx_stdio_filebuf=no)
 AC_LANG_POP([C++])
])

AC_CACHE_CHECK(for the 4-argument constructor of __gnu_cxx::stdio_filebuf,
ac_cv_cxx_old_gnucxx_stdio_filebuf,
[AC_LANG_PUSH([C++])
 AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <ext/stdio_filebuf.h>
#include <istream>
int fd = 0;
__gnu_cxx::stdio_filebuf<char> *fb = new __gnu_cxx::stdio_filebuf<char>(fd,std::ios_base::in,true,1);
]])],
 ac_cv_cxx_old_gnucxx_stdio_filebuf=yes,
 ac_cv_cxx_old_gnucxx_stdio_filebuf=no)
 AC_LANG_POP([C++])
])

if test "$ac_cv_cxx_new_gnucxx_stdio_filebuf" = yes; then
  AC_DEFINE(HAVE_GNUCXX_NEW_STDIO_FILEBUF,,[define if the compiler supports the 2-argument constructor for __gnu_cxx::stdio_filebuf])
elif test "$ac_cv_cxx_old_gnucxx_stdio_filebuf" = yes; then
  AC_DEFINE(HAVE_GNUCXX_OLD_STDIO_FILEBUF,,[define if the compiler supports the 4-argument constructor for __gnu_cxx::stdio_filebuf])
else
  AC_MSG_FAILURE([Could not find any __gnu_cxx::stdio_filebuf])
fi
])
