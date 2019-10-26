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

#ifndef HERON_API_TOPOLOGY_HERON_SUBMITTER_H_
#define HERON_API_TOPOLOGY_HERON_SUBMITTER_H_

#include <fstream>
#include <map>
#include <string>

#include "proto/messages.h"
#include "utils/utils.h"

namespace heron {
namespace api {
namespace topology {

class HeronSubmitter {
 public:
  static void submitTopology(const proto::api::Topology& topology) {
    if (!topology.IsInitialized()) {
      throw std::invalid_argument("topology structure not initialized");
    }
    std::map<std::string, std::string> cmdLineOptions;
    utils::Utils::readCommandLineOpts(cmdLineOptions);
    if (cmdLineOptions.find("cmdline.topologydefn.tmpdirectory") !=
        cmdLineOptions.end()) {
      submitTopologyToFile(topology, cmdLineOptions["cmdline.topologydefn.tmpdirectory"]);
    } else {
      throw std::invalid_argument("heron submit has to have command line options");
    }
  }

 private:
  static void submitTopologyToFile(const proto::api::Topology& topology,
                                   const std::string& dirName) {
    std::string fileName = dirName + "/" + topology.name() + ".defn";
    std::ofstream fd;
    fd.open(fileName, std::ios::binary|std::ios::out);
    if (!topology.SerializeToOstream(&fd)) {
      throw std::invalid_argument("topology defn could not be written to file " + fileName);
    }
    fd.close();
  }

  HeronSubmitter() { }
};

}  // namespace topology
}  // namespace api
}  // namespace heron

#endif  // HERON_API_TOPOLOGY_HERON_SUBMITTER_H_
