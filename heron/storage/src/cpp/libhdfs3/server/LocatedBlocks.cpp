/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "Exception.h"
#include "ExceptionInternal.h"
#include "LocatedBlock.h"
#include "LocatedBlocks.h"

#include <algorithm>
#include <cassert>
#include <iostream>

namespace Hdfs {
namespace Internal {

const LocatedBlock * LocatedBlocksImpl::findBlock(int64_t position) {
    if (position < fileLength) {
        LocatedBlock target(position);
        std::vector<LocatedBlock>::iterator bound;

        if (blocks.empty() || position < blocks.begin()->getOffset()) {
            return NULL;
        }

        /*
         * bound is first block which start offset is larger than
         * or equal to position
         */
        bound = std::lower_bound(blocks.begin(), blocks.end(), target,
                                 std::less<LocatedBlock>());
        assert(bound == blocks.end() || bound->getOffset() >= position);
        LocatedBlock * retval = NULL;

        if (bound == blocks.end()) {
            retval = &blocks.back();
        } else if (bound->getOffset() > position) {
            assert(bound != blocks.begin());
            --bound;
            retval = &(*bound);
        } else {
            retval = &(*bound);
        }

        if (position < retval->getOffset()
                || position >= retval->getOffset() + retval->getNumBytes()) {
            return NULL;
        }

        return retval;
    } else {
        return lastBlock.get();
    }
}

}
}
