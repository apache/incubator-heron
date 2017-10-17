//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.uploader.dlog;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;

final class DLContext extends Context {

  public static final String DL_TOPOLOGIES_NS_URI = "heron.uploader.dlog.topologies.namespace.uri";
  public static final String DL_TOPOLOGIES_NUM_REPLICAS =
      "heron.uploader.dlog.topologies.num.replicas";

  private DLContext() {
  }

  public static String dlTopologiesNamespaceURI(Config config) {
    return config.getStringValue(DL_TOPOLOGIES_NS_URI);
  }

  public static int dlTopologiesNumReplicas(Config config) {
    // by default save 3 replicas for each topology
    return config.getIntegerValue(DL_TOPOLOGIES_NUM_REPLICAS, 3);
  }

}
