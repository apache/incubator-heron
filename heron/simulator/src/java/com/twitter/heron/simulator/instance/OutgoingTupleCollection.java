// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.simulator.instance;

import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.proto.system.HeronTuples;

/**
 * Implements OutgoingTupleCollection will be able to handle some basic methods for send out tuples
 * 1. initNewControlTuple or initNewDataTuple
 * 2. addDataTuple, addAckTuple and addFailTuple
 * 3. flushRemaining tuples and sent out the tuples
 * <p>
 * In fact, when talking about to send out tuples, we mean we push them to the out queues.
 */
public class OutgoingTupleCollection extends com.twitter.heron.instance.OutgoingTupleCollection {

  public OutgoingTupleCollection(PhysicalPlanHelper helper,
                                 Communicator<HeronTuples.HeronTupleSet> outQueue) {
    super(helper, outQueue);
  }
}
