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

import com.google.protobuf.Message;

import com.twitter.heron.api.bolt.IOutputCollector;
import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.utils.metrics.BoltMetrics;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;

/**
 * BoltOutputCollectorImpl is used by bolt to emit tuples, it contains:
 * 1. IPluggableSerializer serializer, which will define the serializer
 * 2. OutgoingTupleCollection outputter.
 * When a tuple is to be emitted, it will serialize it and call OutgoingTupleCollection.admitBoltTuple()
 * to sent it out.
 * <p>
 * It will handle the extra work to emit a tuple:
 * For data tuples:
 * 1. Set the anchors for a tuple
 * 2. Pack the tuple and submit the OutgoingTupleCollection's addDataTuple
 * 3. Update the metrics
 * <p>
 * For Control tuples (ack &amp; fail):
 * 1. Set the anchors for a tuple
 * 2. Pack the tuple and submit the OutgoingTupleCollection's addDataTuple
 * 3. Update the metrics
 */
public class BoltOutputCollectorImpl
    extends com.twitter.heron.instance.bolt.BoltOutputCollectorImpl
    implements IOutputCollector {

  public BoltOutputCollectorImpl(IPluggableSerializer serializer,
                                 PhysicalPlanHelper helper,
                                 Communicator<Message> streamOutQueue,
                                 BoltMetrics boltMetrics) {
    super(serializer, helper, streamOutQueue, boltMetrics);
  }
}
