//  Copyright 2016 Twitter. All rights reserved.
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
package com.twitter.heron.scheduler.dryrun;

/**
 * Raw dry-run render
 */
public class RawDryRunRender extends DryRunRender {

  public RawDryRunRender() {
  }

  private String common(SubmitDryRunResponse response) {
    StringBuilder builder = new StringBuilder();
    builder.append("Packing class: " + response.getPackingClass() + "\n");
    builder.append("Topology: ");
    builder.append(response.getTopologyName());
    builder.append('\n');
    builder.append("Packing plan:\n");
    builder.append(response.getPackingPlan().toString());
    return builder.toString();
  }

  @Override
  public String render(SubmitDryRunResponse response) {
    return common(response);
  }

  @Override
  public String render(UpdateDryRunResponse response) {
    StringBuilder builder = new StringBuilder();
    builder.append(common(response));
    builder.append('\n');
    builder.append("Old packing plan:\n");
    builder.append(response.getOldPackingPlan().toString());
    return builder.toString();
  }
}
