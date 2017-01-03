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

package com.twitter.heron.api.spout;

import java.util.Map;

import com.twitter.heron.api.topology.IStatefulComponent;
import com.twitter.heron.api.topology.TopologyContext;

/**
 * ISpout is the core interface for implementing spouts. A Spout is responsible
 * for feeding messages into the topology for processing. For every tuple emitted by
 * a spout, Heron will track the (potentially very large) DAG of tuples generated
 * based on a tuple emitted by the spout. When Heron detects that every tuple in
 * that DAG has been successfully processed, it will send an ack message to the Spout.
 * <p>
 * <p>If a tuple fails to be fully process within the configured timeout for the
 * topology (see {@link com.twitter.heron.api.Config}), Heron will send a fail
 * message to the spout
 * for the message.</p>
 * <p>
 * <p> When a Spout emits a tuple, it can tag the tuple with a message id. The message id
 * can be any type. When Heron acks or fails a message, it will pass back to the
 * spout the same message id to identify which tuple it's referring to. If the spout leaves out
 * the message id, or sets it to null, then Heron will not track the message and the spout
 * will not receive any ack or fail callbacks for the message.</p>
 * <p>
 * <p>Heron executes ack, fail, and nextTuple all on the same thread. This means that an implementor
 * of an ISpout does not need to worry about concurrency issues between those methods. However, it
 * also means that an implementor must ensure that nextTuple is non-blocking: otherwise
 * the method could await acks and fails that are pending to be processed.</p>
 */
public interface IStatefulSpout extends IStatefulComponent, ISpout {
}
