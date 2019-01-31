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

package org.apache.heron.healthmgr.detectors;

import com.microsoft.dhalion.api.IDetector;
import com.microsoft.dhalion.policy.PoliciesExecutor.ExecutionContext;

public abstract class BaseDetector implements IDetector {
  protected ExecutionContext context;

  public enum SymptomType {
    SYMPTOM_COMP_BACK_PRESSURE(BackPressureDetector.class.getSimpleName() + "Component"),
    SYMPTOM_INSTANCE_BACK_PRESSURE(BackPressureDetector.class.getSimpleName() + "Instance"),
    SYMPTOM_GROWING_WAIT_Q(GrowingWaitQueueDetector.class.getSimpleName()),
    SYMPTOM_LARGE_WAIT_Q(LargeWaitQueueDetector.class.getSimpleName()),
    SYMPTOM_PROCESSING_RATE_SKEW(ProcessingRateSkewDetector.class.getSimpleName()),
    SYMPTOM_WAIT_Q_SIZE_SKEW(WaitQueueSkewDetector.class.getSimpleName());

    private String text;

    SymptomType(String name) {
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

  @Override
  public void initialize(ExecutionContext ctxt) {
    this.context = ctxt;
  }
}
