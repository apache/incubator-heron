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

package com.twitter.heron.statemgr.localfs;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;

public class LocalFileSystemContext extends Context {

  /**
   * Get the config specifying whether to initialize file directory hierarchy
   *
   * @param config the config map
   * @return true if config does not exist, else the specified value
   */
  public static boolean initLocalFileTree(Config config) {
    return config.getBooleanValue(
        LocalFileSystemKeys.initializeFileTree(), true);
  }
}
