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

#include <memory>
#include <string>

#include "spouts/test-word-spout.h"
#include "config/config.h"
#include "topology/task-context.h"
#include "bolt/base-rich-bolt.h"
#include "bolt/ibolt-output-collector.h"
#include "topology/output-fields-declarer.h"
#include "topology/topology-builder.h"
#include "topology/heron-submitter.h"
#include "tuple/tuple.h"

namespace heron {
namespace examples {

class ExclamationBolt : public api::bolt::BaseRichBolt {
 public:
  void open(std::shared_ptr<api::config::Config> conf,
            std::shared_ptr<api::topology::TaskContext> context,
            std::shared_ptr<api::bolt::IBoltOutputCollector> collector) {
    nItems_ = 0;
    context_ = context;
  }

  void execute(std::shared_ptr<api::tuple::Tuple> tup) {
    auto value = std::make_tuple<std::string>("");
    tup->getValues(value);
    if (++nItems_ % 100000 == 0) {
      logger_ << std::get<0>(value) << "!!!";
      context_->log(logger_);
      logger_ << "Processed " << ++nItems_ << " items";
      context_->log(logger_);
    }
  }

  void declareOutputFields(std::shared_ptr<api::topology::OutputFieldsDeclarer> declarer) {
  }

 private:
  int64_t nItems_;
  std::shared_ptr<api::topology::TaskContext> context_;
  std::ostringstream logger_;
};

extern "C" {
ExclamationBolt* createExclamationBolt() {
  return new ExclamationBolt();
}
}

}  // namespace examples
}  // namespace heron

int main(int argc, char* argv[]) {
  auto builder = std::make_shared<heron::api::topology::TopologyBuilder>();
  int parallelism = 1;
  builder->setSpout("word", std::make_shared<heron::examples::TestWordSpout>(),
                    "createTestWordSpout", parallelism);
  builder->setBolt("exclaim1", std::make_shared<heron::examples::ExclamationBolt>(),
                   "createExclamationBolt", 4 * parallelism)-> shuffleGrouping("word");

  auto conf = std::make_shared<heron::api::config::Config>();
  conf->setDebug(true);
  conf->setMaxSpoutPending(10);
  conf->setComponentRam("word", 3l * 1024 * 1024 * 1024);
  conf->setComponentRam("exclaim1", 5l * 1024 * 1024 * 1024);
  conf->setContainerDiskRequested(5l * 1024 * 1024 * 1024);
  conf->setContainerCpuRequested(5);
  conf->setNumStmgrs(parallelism);
  if (argc > 1) {
    heron::api::topology::HeronSubmitter::submitTopology(builder->createTopology(argv[1], conf));
  } else {
    throw std::invalid_argument("Topology name must be specified");
  }
  return 0;
}
