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

import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.api.spout.ISpoutOutputCollector;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.utils.metrics.ComponentMetrics;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;

/**
 * SpoutOutputCollectorImpl is used by bolt to emit tuples, it contains:
 * 1. IPluggableSerializer serializer, which will define the serializer
 * 2. OutgoingTupleCollection outputter.
 * When a tuple is to be emitted, it will serialize it and call OutgoingTupleCollection.admitSpoutTuple()
 * to sent it out.
 * <p>
 * It will only emit data tuples; it will not send control tuples (ack&amp;fail)
 * 1. Whether some tuples are expired; should be considered as failed automatically
 * 2. The pending tuples to be acked
 * 3. Maintain some statistics, for instance, total tuples emitted.
 * <p>
 */
public class SpoutOutputCollectorImpl
    extends com.twitter.heron.instance.spout.SpoutOutputCollectorImpl
    implements ISpoutOutputCollector {

  public SpoutOutputCollectorImpl(IPluggableSerializer serializer,
                                  PhysicalPlanHelper helper,
                                  Communicator<Message> streamOutQueue,
                                  ComponentMetrics spoutMetrics) {
    super(serializer, helper, streamOutQueue, spoutMetrics);
  }
}
