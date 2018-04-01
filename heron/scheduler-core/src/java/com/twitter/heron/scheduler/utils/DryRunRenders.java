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
package com.twitter.heron.scheduler.utils;

import com.twitter.heron.common.basics.DryRunFormatType;
import com.twitter.heron.scheduler.dryrun.SubmitDryRunResponse;
import com.twitter.heron.scheduler.dryrun.SubmitRawDryRunRenderer;
import com.twitter.heron.scheduler.dryrun.SubmitTableDryRunRenderer;
import com.twitter.heron.scheduler.dryrun.UpdateDryRunResponse;
import com.twitter.heron.scheduler.dryrun.UpdateRawDryRunRenderer;
import com.twitter.heron.scheduler.dryrun.UpdateTableDryRunRenderer;

public final class DryRunRenders {

  public static String render(SubmitDryRunResponse response, DryRunFormatType formatType) {
    switch (formatType) {
      case RAW :
        return new SubmitRawDryRunRenderer(response).render();
      case TABLE:
        return new SubmitTableDryRunRenderer(response, false).render();
      case COLORED_TABLE:
        return new SubmitTableDryRunRenderer(response, true).render();
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
      default: throw new IllegalArgumentException(
          String.format("Unexpected rendering format: %s", formatType));
    }
  }

  private DryRunRenders() {
  }
}
