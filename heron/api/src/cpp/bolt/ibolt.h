/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef HERON_API_BOLT_IBOLT_H_
#define HERON_API_BOLT_IBOLT_H_

#include <string>
#include <memory>

#include "config/config.h"
#include "topology/task-context.h"
#include "tuple/tuple.h"
#include "bolt/ibolt-output-collector.h"

namespace heron {
namespace api {
namespace bolt {

/**
 * An IBolt represents a component that takes tuples as input and produces tuples
 * as output. An IBolt can do everything from filtering to joining to functions
 * to aggregations. It does not have to process a tuple immediately and may
 * hold onto tuples to process later.
 * <p>
 * <p>A bolt's lifecycle is as follows:</p>
 * <p>
 * <p>IBolt object create function is specified during topology building time.
 * Using that function the IBolt is created on the bolt instance. The system calls
 * prepare on it, and then start processing tuples.</p>
 * <p>
 * <p>When defining bolts in C++, you should use the IRichBolt interface which adds
 * necessary methods for using the C++ TopologyBuilder API.</p>
 */
class IBolt {
 public:
  /**
   * Called when a task for this component is initialized on the cluster.
   * It provides the bolt with the environment in which the bolt executes.
   * <p>
   * <p>This includes the:</p>
   *
   * @param conf The Heron configuration for this bolt. This is the configuration provided
   * to the topology merged in with cluster configuration on this machine.
   * @param context This object can be used to get information about this task's place within
   * the topology, including the task id and component id of this task, input and output
   * information, etc.
   * @param collector The collector is used to emit tuples from this bolt. Tuples can be emitted
   * at any time, including the prepare and cleanup methods. The collector is thread-safe and
   * should be saved as an instance variable of this bolt object.
   */
  virtual void open(std::shared_ptr<config::Config> conf,
                    std::shared_ptr<topology::TaskContext> context,
                    std::shared_ptr<IBoltOutputCollector> collector) = 0;

  /**
   * Process a single tuple of input. The Tuple object contains metadata on it
   * about which component/stream/task it came from. The values of the Tuple can
   * be accessed using Tuple#getValues. The IBolt does not have to process the Tuple
   * immediately. It is perfectly fine to hang onto a tuple and process it later
   * (for instance, to do an aggregation or join).
   * <p>
   * <p>Tuples should be emitted using the OutputCollector provided through the prepare method.
   * It is required that all input tuples are acked or failed at some point using the OutputCollector.
   * Otherwise, Heron will be unable to determine when tuples coming off the spouts
   * have been completed.</p>
   * <p>
   * <p>For the common case of acking an input tuple at the end of the execute method,
   * see IBasicBolt which automates this.</p>
   *
   * @param input The input tuple to be processed.
   */
  virtual void execute(std::shared_ptr<tuple::Tuple> input) = 0;

  /**
   * Called when an IBolt is going to be shutdown. There is no guarentee that cleanup
   * will be called, because the supervisor kill -9's worker processes on the cluster.
   * <p>
   * <p>The one context where cleanup is guaranteed to be called is when a topology
   * is killed when running Heron in simulator.</p>
   */
  virtual void cleanup() = 0;

  /**
   * C++ needs a destructor
   */
  virtual ~IBolt() { }
};

}  // namespace bolt
}  // namespace api
}  // namespace heron

#endif  // HERON_API_BOLT_IBOLT_H_
