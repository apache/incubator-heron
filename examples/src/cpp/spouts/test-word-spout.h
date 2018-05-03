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

#ifndef HERON_EXAMPLES_SPOUTS_TEST_WORD_SPOUT_H_
#define HERON_EXAMPLES_SPOUTS_TEST_WORD_SPOUT_H_

#include <unistd.h>

#include <map>
#include <string>
#include <tuple>
#include <vector>
#include <memory>

#include "spout/base-rich-spout.h"
#include "spout/ispout-output-collector.h"
#include "topology/task-context.h"
#include "topology/output-fields-declarer.h"
#include "tuple/fields.h"

namespace heron {
namespace examples {

class TestWordSpout : public api::spout::BaseRichSpout {
 public:
  TestWordSpout() : nSent_(0), nAcks_(0), nFails_(0) { }
  virtual ~TestWordSpout() { }
  virtual void open(std::shared_ptr<api::config::Config> config,
                    std::shared_ptr<api::topology::TaskContext> context,
                    std::shared_ptr<api::spout::ISpoutOutputCollector> collector) {
    collector_ = collector;
    context_ = context;
    words_ = {"nathan", "mike", "jackson", "golda", "bertels"};
    iter_ = words_.begin();
  }

  void close() {
  }

  virtual void nextTuple() {
    auto tup = std::make_tuple(*iter_);
    collector_->emit(tup, ++nSent_);
    ++iter_;
    if (iter_ == words_.end()) { iter_ = words_.begin(); }
    logger_ << "TestWordSpout emitted " << nSent_
            << " and got acks for "
            << nAcks_ << " and fails for " << nFails_;
    context_->log(logger_);
    if (nSent_ % 100000 == 0) {
      logger_ << "TestWordSpout emitted " << nSent_
              << " and got acks for "
              << nAcks_ << " and fails for " << nFails_;
      context_->log(logger_);
    }
  }

  virtual void ack(int64_t) {
    ++nAcks_;
  }
  virtual void fail(int64_t) {
    ++nFails_;
  }

  virtual void declareOutputFields(std::shared_ptr<api::topology::OutputFieldsDeclarer> declarer) {
    api::tuple::Fields flds({"word"});
    declarer->declare(flds);
  }

 private:
  std::vector<std::string> words_;
  std::vector<std::string>::iterator iter_;
  std::shared_ptr<api::spout::ISpoutOutputCollector> collector_;
  std::shared_ptr<api::topology::TaskContext> context_;
  int64_t nSent_, nAcks_, nFails_;
  std::ostringstream logger_;
};

extern "C" {
TestWordSpout* createTestWordSpout() {
  return new TestWordSpout();
}
}

}  // namespace examples
}  // namespace heron

#endif  // HERON_EXAMPLES_SPOUTS_TEST_WORD_SPOUT_H_
