/********************************************************************\
 * command.cpp -- command line interface                            *
 *                                                                  *
 * Copyright (C) 2009 Kenneth Laskoski                              *
 *                                                                  *
\********************************************************************/
/** @file command.cpp
    @brief command line interface
    @author Copyright (C) 2009 Kenneth Laskoski
    based on work by
    @author Copyright (C) 2004-2008 Ralf S. Engelschall <rse@engelschall.com>

    Use, modification, and distribution are subject
    to the Boost Software License, Version 1.0.  (See accompanying file
    LICENSE_1_0.txt or a copy at <http://www.boost.org/LICENSE_1_0.txt>.)
*/

#include "core/kashmir/public/uuid.h"
#include "core/kashmir/public/polydevrandom.h"

#include <iostream>
#include <fstream>

#include <unistd.h>

namespace
{
    using kashmir::uuid_t;
    using kashmir::system::PolyDevRandom;
    using kashmir::user::AbstractRandomStream;

    int n = 1;

    std::ostream* outp = &std::cout;
    std::ofstream ofile;

    void parse_cmd_line(int argc, char *argv[])
    {
        int ch;
        char *p;

        while ((ch = getopt(argc, argv, "n:o:")) != -1) {
            switch (ch) {
                case 'n':
                    n = strtoul(optarg, &p, 10);
                    if (*p != '\0' || n < 1)
                        std::cerr << "invalid argument to option 'n'\n";
                    break;
                case 'o':
                    ofile.open(optarg);
                    outp = &ofile;
                    break;
                default:
                    exit(1);
            }
        }
        argv += optind;
        argc -= optind;
    }
}

int main(int argc, char *argv[])
{
    parse_cmd_line(argc, argv);

    PolyDevRandom devrandom;

    AbstractRandomStream& in = devrandom;
    std::ostream& out = *outp;

    kashmir::uuid_t uuid;
    for (int i = 0; i < n; i++) {
        in >> uuid;
        out << uuid << '\n';
    }

    return 0;
}
