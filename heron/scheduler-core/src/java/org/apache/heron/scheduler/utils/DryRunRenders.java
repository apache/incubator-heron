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

package org.apache.heron.scheduler.utils;

import org.apache.heron.common.basics.DryRunFormatType;
import org.apache.heron.scheduler.dryrun.SubmitDryRunResponse;
import org.apache.heron.scheduler.dryrun.SubmitJsonDryRunRenderer;
import org.apache.heron.scheduler.dryrun.SubmitRawDryRunRenderer;
import org.apache.heron.scheduler.dryrun.SubmitTableDryRunRenderer;
import org.apache.heron.scheduler.dryrun.UpdateDryRunResponse;
import org.apache.heron.scheduler.dryrun.UpdateJsonDryRunRenderer;
import org.apache.heron.scheduler.dryrun.UpdateRawDryRunRenderer;
import org.apache.heron.scheduler.dryrun.UpdateTableDryRunRenderer;

public final class DryRunRenders {

  public static String render(SubmitDryRunResponse response, DryRunFormatType formatType) {
    switch (formatType) {
      case RAW :
        return new SubmitRawDryRunRenderer(response).render();
      case TABLE:
        return new SubmitTableDryRunRenderer(response, false).render();
      case COLORED_TABLE:
        return new SubmitTableDryRunRenderer(response, true).render();
      case JSON:
        return new SubmitJsonDryRunRenderer(response).render();
      default: throw new IllegalArgumentException(
          String.format("Unexpected rendering format: %s", formatType));
    }
  }

  public static String render(UpdateDryRunResponse response, DryRunFormatType formatType) {
    switch (formatType) {
      case RAW :
        return new UpdateRawDryRunRenderer(response).render();
      case TABLE:
        return new UpdateTableDryRunRenderer(response, false).render();
      case COLORED_TABLE:
        return new UpdateTableDryRunRenderer(response, true).render();
      case JSON:
        return new UpdateJsonDryRunRenderer(response).render();
      default: throw new IllegalArgumentException(
          String.format("Unexpected rendering format: %s", formatType));
    }
  }

  private DryRunRenders() {
  }
}
