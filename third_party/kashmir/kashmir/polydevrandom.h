/********************************************************************\
 * polydevrandom.h -- polymorphic UNIX random number generator      *
 *                                                                  *
 * Copyright (C) 2009 Kenneth Laskoski                              *
 *                                                                  *
\********************************************************************/
/** @file polydevrandom.h
    @brief polymorphic UNIX random number generator
    @author Copyright (C) 2009 Kenneth Laskoski
    based on work by
    @author Copyright (C) 1996, 1997, 1998 Theodore Ts'o
    @author Copyright (C) 2004-2008 Ralf S. Engelschall <rse@engelschall.com>

    Use, modification, and distribution are subject
    to the Boost Software License, Version 1.0.  (See accompanying file
    LICENSE_1_0.txt or a copy at <http://www.boost.org/LICENSE_1_0.txt>.)
*/

#ifndef KL_POLYDEVRANDOM_H
#define KL_POLYDEVRANDOM_H

#include "core/kashmir/public/abstractrandomstream.h"

#include <fstream>
#include <stdexcept>

namespace kashmir { namespace system {

class PolyDevRandom : public user::AbstractRandomStream
{
 public:

  PolyDevRandom() : file("/dev/urandom", std::ios::binary)
  {
    if (!file) throw std::runtime_error("failed to open random device.");
  }

  void 
  read(char* buffer, std::size_t count) { file.read(buffer, count); }

  PolyDevRandom(const PolyDevRandom& ) = delete;
  const PolyDevRandom& operator=(const PolyDevRandom& ) = delete;

 private:
  std::ifstream file;
};

}}

#endif
