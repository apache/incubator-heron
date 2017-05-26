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
//  limitations under the License
package com.twitter.heron.scheduler.kubernetes;

import java.util.logging.Logger;

/**
 * Created by john on 5/26/17.
 */
public class KubernetesController {
  private static final Logger LOG = Logger.getLogger(KubernetesController.class.getName());

  private final String kubernetesURI;
  private final String topologyName;
  private final boolean isVerbose;

  public KubernetesController(
      String kubernetesURI,
      String topologyName,
      boolean isVerbose
  ) {
    this.kubernetesURI = kubernetesURI;
    this.topologyName = topologyName;
    this.isVerbose = isVerbose;
  }

  public boolean killTopology() {
    return true;
  }

  public boolean restartApp(int appId) {
    return true;
  }

  public boolean submitTopology(String appConf) {
    return true;
  }


}
