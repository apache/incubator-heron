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
package com.twitter.heron.integration_test.core;

import java.io.Serializable;

// We keep this since we want to be consistent with earlier framework to reuse test topologies
public interface ITerminalBolt extends Serializable {
  /**
   * Invoke to write all aggregated data to the destination
   * Destination can be http URL, local file, hdfs, etc.
   */
  void writeFinishedData();
}
