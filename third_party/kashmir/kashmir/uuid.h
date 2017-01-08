/********************************************************************\
 * uuid.h -- universally unique ID - as defined by ISO/IEC 9834-8   *
 *                                                                  *
 * Copyright (C) 2008 Kenneth Laskoski                              *
 *                                                                  *
\********************************************************************/
/** @file uuid.h
    @brief universally unique ID - as defined by ISO/IEC 9834-8:2005
    @author Copyright (C) 2008 Kenneth Laskoski
    based on work by
    @author Copyright (C) 2006 Andy Tompkins
    @author Copyright (C) 2000 Dave Peticolas <peticola@cs.ucdavis.edu>
    @author Copyright (C) 1996, 1997, 1998 Theodore Ts'o
    @author Copyright (C) 2004-2008 Ralf S. Engelschall <rse@engelschall.com>

    Use, modification, and distribution are subject
    to the Boost Software License, Version 1.0.  (See accompanying file
    LICENSE_1_0.txt or a copy at <http://www.boost.org/LICENSE_1_0.txt>.)
*/
#ifndef KL_UUID_H
#define KL_UUID_H

#include <istream>
#include <ostream>
#include <sstream>
#include "kashmir/randomstream.h"

#include <cstddef>

#include <stdexcept>
#include <algorithm>

#include "kashmir/iostate.h"

namespace kashmir {
namespace uuid {

/** @class uuid_t
    @brief This class provides a C++ binding to the UUID type defined in
    - ISO/IEC 9834-8:2005 | ITU-T Rec. X.667 - available at http://www.itu.int/ITU-T/studygroups/com17/oid.html
    - IETF RFC 4122 - available at http://tools.ietf.org/html/rfc4122

    These technically equivalent standards document the code below.
*/

class uuid_t
{
    // an UUID is a string of 16 octets (128 bits)
    typedef std::size_t size_type;
    static const size_type size = 16;
    static const size_type string_size = 36; // XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX

    // we use an unpacked representation, value_type may be larger than 8 bits,
    // in which case every input operation must assert data[i] < 256 for i < 16
    // note even char may be more than 8 bits in some particular platform
    typedef unsigned char value_type;
    value_type data[size];

    // test for "nil" value
    bool is_nil() const
    {
        for (size_type i = 0; i < size; ++i)
            if (data[i])
                return false;
        return true;
    }

public:

    // default value is "nil"
    uuid_t()
    {
        // std::fill(data, data+size, 0);
    }

    // destruction, copy and assignment
    ~uuid_t() {}

    uuid_t(const uuid_t& rhs)
    {
        std::copy(rhs.data, rhs.data+size, data);
    }

    uuid_t& operator=(const uuid_t& rhs)
    {
        std::copy(rhs.data, rhs.data+size, data);
        return *this;
    }

    // initialization from string literal
    explicit uuid_t(const char* literal)
    {
        std::stringstream input(literal);
        this->get(input);
    }

    // comparison operators define a total order
    bool operator==(const uuid_t& rhs) const
    {
        return std::equal(data, data+size, rhs.data);
    }

    bool operator<(const uuid_t& rhs) const
    {
        return std::lexicographical_compare(data, data+size, rhs.data, rhs.data+size);
    }

    bool operator>(const uuid_t& rhs) const { return (rhs < *this); }
    bool operator<=(const uuid_t& rhs) const { return !(rhs < *this); }
    bool operator>=(const uuid_t& rhs) const { return !(*this < rhs); }
    bool operator!=(const uuid_t& rhs) const { return !(*this == rhs); }

    // some syntatic sugar using the is_nil method
    bool operator!() const { return is_nil(); }

    typedef bool (uuid_t::*bool_type)() const;
    operator bool_type() const
    {
        return is_nil() ? 0 : &uuid_t::is_nil;
    }

    // insertion and extraction
    template<class char_t, class char_traits>
    std::basic_ostream<char_t, char_traits>& put(std::basic_ostream<char_t, char_traits>& os) const;

    template<class char_t, class char_traits>
    std::basic_istream<char_t, char_traits>& get(std::basic_istream<char_t, char_traits>& is);

    // version 4 uuid extraction from a random stream
    template<class user_impl>
    user::randomstream<user_impl>& get(user::randomstream<user_impl>& is);
};

template<class char_t, class char_traits>
std::basic_ostream<char_t, char_traits>& uuid_t::put(std::basic_ostream<char_t, char_traits>& os) const
{
    if (!os.good())
        return os;

    const typename std::basic_ostream<char_t, char_traits>::sentry ok(os);
    if (ok)
    {
        ios_flags_saver flags(os);
        basic_ios_fill_saver<char_t, char_traits> fill(os);

        const std::streamsize width = os.width(0);
        const std::streamsize mysize = string_size;

        // right padding
        if (flags.value() & (std::ios_base::right | std::ios_base::internal))
            for (std::streamsize i = width; i > mysize; --i)
                os << fill.value();

        os << std::hex;
        os.fill(os.widen('0'));

        for (size_t i = 0; i < 16; ++i)
        {
            os.width(2);
            os << static_cast<unsigned>(data[i]);
            // if (i == 3 || i == 5 || i == 7 || i == 9)
            //    os << os.widen('-');
        }

        // left padding
        if (flags.value() & std::ios_base::left)
            for (std::streamsize i = width; i > mysize; --i)
                os << fill.value();
    }

    return os;
}

template<class char_t, class char_traits>
std::basic_istream<char_t, char_traits>& uuid_t::get(std::basic_istream<char_t, char_traits>& is)
{
    if (!is.good())
        return is;

    const typename std::basic_istream<char_t, char_traits>::sentry ok(is);
    if (ok)
    {
        char_t hexdigits[16];
        char_t* const npos = hexdigits+16;

        typedef std::ctype<char_t> facet_t;
        const facet_t& facet = std::use_facet<facet_t>(is.getloc());

        const char* tmp = "0123456789abcdef";
        facet.widen(tmp, tmp+16, hexdigits);

        char_t c;
        char_t* f;
        for (size_t i = 0; i < size; ++i)
        {
            is >> c;
            c = facet.tolower(c);

            f = std::find(hexdigits, npos, c);
            if (f == npos)
            {
                is.setstate(std::ios_base::failbit);
                break;
            }

            data[i] = static_cast<value_type>(std::distance(hexdigits, f));

            is >> c;
            c = facet.tolower(c);

            f = std::find(hexdigits, npos, c);
            if (f == npos)
            {
                is.setstate(std::ios_base::failbit);
                break;
            }

            data[i] <<= 4;
            data[i] |= static_cast<value_type>(std::distance(hexdigits, f));

            if (i == 3 || i == 5 || i == 7 || i == 9)
            {
                is >> c;
                if (c != is.widen('-'))
                {
                    is.setstate(std::ios_base::failbit);
                    break;
                }
            }
        }

        if (!is)
            throw std::runtime_error("failed to extract valid uuid from stream.");
    }

    return is;
}

template<class user_impl>
user::randomstream<user_impl>& uuid_t::get(user::randomstream<user_impl>& is)
{
    // get random bytes
    char buffer[size];
    is.read(buffer, size);
    std::copy(buffer, buffer+size, data);

    // this loop is necessary if uuid_t::value_type is larger than 8 bits,
    // in order to maintain the invariant data[i] < 256 for i < 16
    // note even char may be more than 8 bits in some particular platform
//    for (size_t i = 0; i < size; ++i)
//        data[i] &= 0xff;

    // set variant
    // should be 0b10xxxxxx
    data[8] &= 0xbf;   // 0b10111111
    data[8] |= 0x80;   // 0b10000000

    // set version
    // should be 0b0100xxxx
    data[6] &= 0x4f;   // 0b01001111
    data[6] |= 0x40;   // 0b01000000

    return is;
}

template<class char_t, class char_traits>
inline std::basic_ostream<char_t, char_traits>& operator<<(std::basic_ostream<char_t, char_traits>& os, const uuid_t& uuid)
{
    return uuid.put(os);
}

template<class char_t, class char_traits>
inline std::basic_istream<char_t, char_traits>& operator>>(std::basic_istream<char_t, char_traits>& is, uuid_t& uuid)
{
    return uuid.get(is);
}

template<class user_impl>
inline user::randomstream<user_impl>& operator>>(user::randomstream<user_impl>& is, uuid_t& uuid)
{
    return uuid.get(is);
}

} // namespace kashmir::uuid

using uuid::uuid_t;

} // namespace kashmir

#endif
