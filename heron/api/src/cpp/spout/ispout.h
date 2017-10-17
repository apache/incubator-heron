/*
 * Copyright 2017 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HERON_API_SPOUT_ISPOUT_H_
#define HERON_API_SPOUT_ISPOUT_H_

#include <string>

#include "config/config.h"
#include "topology/task-context.h"
#include "spout/ispout-output-collector.h"

namespace heron {
namespace api {
namespace spout {

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
 * is a int64. When Heron acks or fails a message, it will pass back to the
 * spout the same message id to identify which tuple it's referring to. If the spout leaves out
 * the message id, or sets it to null, then Heron will not track the message and the spout
 * will not receive any ack or fail callbacks for the message.</p>
 * <p>
 * <p>Heron executes ack, fail, and nextTuple all on the same thread. This means that an implementor
 * of an ISpout does not need to worry about concurrency issues between those methods. However, it
 * also means that an implementor must ensure that nextTuple is non-blocking: otherwise
 * the method could await acks and fails that are pending to be processed.</p>
 */
class ISpout {
 public:
  /**
   * Called when a task for this component is initialized within a worker on the cluster.
   * It provides the spout with the environment in which the spout executes.
   * <p>
   * <p>This includes the:</p>
   *
   * @param conf The Heron configuration for this spout. This is the configuration provided to the
   * topology merged in with cluster configuration on this machine.
   * @param context This object can be used to get information about this task's place within the
   * topology, including the task id and component id of this task, input, output information, etc.
   * @param collector The collector is used to emit tuples from this spout. Tuples can be emitted
   * at any time, including the open and close methods. The collector is thread-safe and should be
   * saved as an instance variable of this spout object.
   */
  virtual void open(std::shared_ptr<config::Config> conf,
                    std::shared_ptr<topology::TaskContext> context,
                    std::shared_ptr<ISpoutOutputCollector> collector) = 0;

  /**
   * Called when an ISpout is going to be shutdown. There is no guarentee that close
   * will be called, because the the instance could be kill -9ed on the cluster.
   * <p>
   * <p>The one context where close is guaranteed to be called is a topology is
   * killed when running Heron in simulator.</p>
   */
  virtual void close() = 0;

  /**
   * Called when a spout has been activated out of a deactivated mode.
   * nextTuple will be called on this spout soon. A spout can become activated
   * after having been deactivated when the topology is manipulated using the
   * `heron` client.
   */
  virtual void activate() = 0;

  /**
   * Called when a spout has been deactivated. nextTuple will not be called while
   * a spout is deactivated. The spout may or may not be reactivated in the future.
   */
  virtual void deactivate() = 0;

  /**
   * When this method is called, Heron is requesting that the Spout emit tuples to the
   * output collector. This method should be non-blocking, so if the Spout has no tuples
   * to emit, this method should return. nextTuple, ack, and fail are all called in a tight
   * loop in a single thread in the spout task. When there are no tuples to emit, it is courteous
   * to have nextTuple sleep for a short amount of time (like a single millisecond)
   * so as not to waste too much CPU.
   */
  virtual void nextTuple() = 0;

  /**
   * Heron has determined that the tuple emitted by this spout with the msgId identifier
   * has been fully processed. Typically, an implementation of this method will take that
   * message off the queue and prevent it from being replayed.
   */
  virtual void ack(int64_t msgId) = 0;

  /**
   * The tuple emitted by this spout with the msgId identifier has failed to be
   * fully processed. Typically, an implementation of this method will put that
   * message back on the queue to be replayed at a later time.
   */
  virtual void fail(int64_t msgId) = 0;

  /**
   * C++ needs a destructor
   */
  virtual ~ISpout() { }
};

}  // namespace spout
}  // namespace api
}  // namespace heron

#endif  // HERON_API_SPOUT_ISPOUT_H_
