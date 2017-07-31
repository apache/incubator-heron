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

package com.twitter.heron.healthmgr.detectors;

import com.microsoft.dhalion.api.IDetector;

public abstract class BaseDetector implements IDetector {
  public enum SymptomName {
    SYMPTOM_BACK_PRESSURE(BackPressureDetector.class.getSimpleName()),
    SYMPTOM_GROWING_WAIT_Q(GrowingWaitQueueDetector.class.getSimpleName()),
    SYMPTOM_LARGE_WAIT_Q(LargeWaitQueueDetector.class.getSimpleName()),
    SYMPTOM_PROCESSING_RATE_SKEW(ProcessingRateSkewDetector.class.getSimpleName()),
    SYMPTOM_WAIT_Q_DISPARITY(WaitQueueDisparityDetector.class.getSimpleName());

    private String text;

    SymptomName(String name) {
      this.text = name;
    }

    public String text() {
      return text;
    }

    @Override
    public String toString() {
      return text();
    }
  }
}
