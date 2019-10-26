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

import com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.packing.PackingPlan;

/**
 * Dry-run renderer that renders submit dry-run response in raw format
 */
public class SubmitJsonDryRunRenderer {
  private final SubmitDryRunResponse response;
  private final JsonFormatterUtils formatter;

  public SubmitJsonDryRunRenderer(SubmitDryRunResponse response) {
    this.response = response;
    this.formatter = new JsonFormatterUtils();
  }

  public String render() {
    String topologyName = response.getTopology().getName();
    String packingClassName = Context.packingClass(response.getConfig());
    PackingPlan packingPlan = response.getPackingPlan();

    try {
      return formatter.renderPackingPlan(topologyName, packingClassName, packingPlan);
    } catch (JsonProcessingException e) {
      return "ERROR: " + e.getMessage();
    }
  }
}
