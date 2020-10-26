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

////////////////////////////////////////////////////////////////
//
// metrics-sinks-reader.h
//
// This file deals with reading metrics sinks yaml file
//
///////////////////////////////////////////////////////////////
#ifndef METIRCS_SINKS_READER_H_
#define METIRCS_SINKS_READER_H_

#include <list>
#include <utility>
#include "config/yaml-file-reader.h"

namespace heron {
namespace config {

class MetricsSinksReader : public YamlFileReader {
 public:
  MetricsSinksReader(std::shared_ptr<EventLoop> eventLoop, const sp_string& _defaults_file);
  virtual ~MetricsSinksReader();

  // Get the list of metrics whitelisted for tmanager along
  // with their types
  void GetTManagerMetrics(std::list<std::pair<sp_string, sp_string> >& metrics);

  virtual void OnConfigFileLoad();
};
}  // namespace config
}  // namespace heron

#endif
