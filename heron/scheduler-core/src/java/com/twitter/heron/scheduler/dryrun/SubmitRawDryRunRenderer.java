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
package com.twitter.heron.scheduler.dryrun;

import com.twitter.heron.spi.common.Context;

/**
 * Dry-run renderer that renders submit dry-run response in raw format
 */
public class SubmitRawDryRunRenderer {
  private final SubmitDryRunResponse response;

  public SubmitRawDryRunRenderer(SubmitDryRunResponse response) {
    this.response = response;
  }

  public String render() {
    StringBuilder builder = new StringBuilder();
    String topologyName = response.getTopology().getName();
    String packingClassName = Context.packingClass(response.getConfig());
    builder.append("Packing class: " + packingClassName + "\n");
    builder.append("Topology: " + topologyName + "\n");
    builder.append("Packing plan:\n");
    builder.append(response.getPackingPlan().toString());
    return builder.toString();
  }
}
