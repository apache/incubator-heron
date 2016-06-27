/********************************************************************\
 * abstractrandomstream.h -- polymorphic random number generator    *
 *                                                                  *
 * Copyright (C) 2009 Kenneth Laskoski                              *
 *                                                                  *
\********************************************************************/
/** @file abstractrandomstream.h
    @brief polymorphic random number generator
    @author Copyright (C) 2009 Kenneth Laskoski

    Use, modification, and distribution are subject
    to the Boost Software License, Version 1.0.  (See accompanying file
    LICENSE_1_0.txt or a copy at <http://www.boost.org/LICENSE_1_0.txt>.)
*/

#ifndef KL_ABSTRACTRANDOMSTREAM_H
#define KL_ABSTRACTRANDOMSTREAM_H

#include "core/kashmir/public/randomstream.h"

namespace kashmir { namespace user {

class AbstractRandomStream : public randomstream<AbstractRandomStream>
{
 public:
   virtual ~AbstractRandomStream() {}
   virtual void read(char* buffer, std::size_t count) = 0;
};

}}

#endif
