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

package org.apache.heron.scheduler.dryrun;

import org.apache.heron.spi.common.Context;

/**
 * Dry-run renderer that renders update dry-run response in table format
 */
public class UpdateRawDryRunRenderer {
  private final UpdateDryRunResponse response;

  public UpdateRawDryRunRenderer(UpdateDryRunResponse response) {
    this.response = response;
  }

  public String render() {
    StringBuilder builder = new StringBuilder();
    String topologyName = response.getTopology().getName();
    String packingClassName = Context.packingClass(response.getConfig());
    builder.append("Packing class: " + packingClassName + "\n");
    builder.append("Topology: " + topologyName + "\n");
    builder.append("New packing plan:\n");
    builder.append(response.getPackingPlan().toString() + "\n");
    builder.append("Old packing plan:\n");
    builder.append(response.getOldPackingPlan().toString() + "\n");
    return builder.toString();
  }
}
